// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "arrow/filesystem/gcsfs.h"

#include <google/cloud/storage/client.h>

#include "arrow/buffer.h"
#include "arrow/filesystem/gcsfs_internal.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/result.h"
#include "arrow/util/checked_cast.h"

#define ARROW_GCS_RETURN_NOT_OK(expr) \
  if (!expr.ok()) return internal::ToArrowStatus(expr)

namespace arrow {
namespace fs {
namespace {

namespace gcs = google::cloud::storage;

auto constexpr kSep = '/';
// Change the default upload buffer size. In general, sending larger buffers is more
// efficient with GCS, as each buffer requires a roundtrip to the service. With formatted
// output (when using `operator<<`), keeping a larger buffer in memory before uploading
// makes sense.  With unformatted output (the only choice given gcs::io::OutputStream's
// API) it is better to let the caller provide as large a buffer as they want. The GCS C++
// client library will upload this buffer with zero copies if possible.
auto constexpr kUploadBufferSize = 256 * 1024;

struct GcsPath {
  std::string full_path;
  std::string bucket;
  std::string object;

  static Result<GcsPath> FromString(const std::string& s) {
    const auto src = internal::RemoveTrailingSlash(s);
    auto const first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return GcsPath{std::string(src), std::string(src), ""};
    }
    GcsPath path;
    path.full_path = std::string(src);
    path.bucket = std::string(src.substr(0, first_sep));
    path.object = std::string(src.substr(first_sep + 1));
    return path;
  }

  bool empty() const { return bucket.empty() && object.empty(); }

  bool operator==(const GcsPath& other) const {
    return bucket == other.bucket && object == other.object;
  }
};

class GcsInputStream : public arrow::io::InputStream {
 public:
  explicit GcsInputStream(gcs::ObjectReadStream stream, std::string bucket_name,
                          std::string object_name, gcs::Generation generation,
                          gcs::ReadFromOffset offset, gcs::Client client)
      : stream_(std::move(stream)),
        bucket_name_(std::move(bucket_name)),
        object_name_(std::move(object_name)),
        generation_(generation),
        offset_(offset.value_or(0)),
        client_(std::move(client)) {}

  ~GcsInputStream() override = default;

  //@{
  // @name FileInterface
  Status Close() override {
    stream_.Close();
    return Status::OK();
  }

  Result<int64_t> Tell() const override {
    if (!stream_) {
      return Status::IOError("invalid stream");
    }
    return stream_.tellg() + offset_;
  }

  bool closed() const override { return !stream_.IsOpen(); }
  //@}

  //@{
  // @name Readable
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    stream_.read(static_cast<char*>(out), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    return stream_.gcount();
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes));
    stream_.read(reinterpret_cast<char*>(buffer->mutable_data()), nbytes);
    ARROW_GCS_RETURN_NOT_OK(stream_.status());
    RETURN_NOT_OK(buffer->Resize(stream_.gcount(), true));
    return buffer;
  }
  //@}

  //@{
  // @name InputStream
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    auto metadata = client_.GetObjectMetadata(bucket_name_, object_name_, generation_);
    ARROW_GCS_RETURN_NOT_OK(metadata.status());
    return internal::FromObjectMetadata(*metadata);
  }
  //@}

 private:
  mutable gcs::ObjectReadStream stream_;
  std::string bucket_name_;
  std::string object_name_;
  gcs::Generation generation_;
  std::int64_t offset_;
  gcs::Client client_;
};

class GcsOutputStream : public arrow::io::OutputStream {
 public:
  explicit GcsOutputStream(gcs::ObjectWriteStream stream) : stream_(std::move(stream)) {}
  ~GcsOutputStream() override = default;

  Status Close() override {
    stream_.Close();
    return internal::ToArrowStatus(stream_.last_status());
  }

  Result<int64_t> Tell() const override {
    if (!stream_) {
      return Status::IOError("invalid stream");
    }
    return tell_;
  }

  bool closed() const override { return !stream_.IsOpen(); }

  Status Write(const void* data, int64_t nbytes) override {
    if (stream_.write(reinterpret_cast<const char*>(data), nbytes)) {
      tell_ += nbytes;
      return Status::OK();
    }
    return internal::ToArrowStatus(stream_.last_status());
  }

  Status Flush() override {
    stream_.flush();
    return Status::OK();
  }

 private:
  gcs::ObjectWriteStream stream_;
  int64_t tell_ = 0;
};

using InputStreamFactory = std::function<Result<std::shared_ptr<io::InputStream>>(
    const std::string&, const std::string&, gcs::Generation, gcs::ReadFromOffset)>;

class GcsRandomAccessFile : public arrow::io::RandomAccessFile {
 public:
  GcsRandomAccessFile(InputStreamFactory factory, gcs::ObjectMetadata metadata,
                      std::shared_ptr<io::InputStream> stream)
      : factory_(std::move(factory)),
        metadata_(std::move(metadata)),
        stream_(std::move(stream)) {}
  ~GcsRandomAccessFile() override = default;

  //@{
  // @name FileInterface
  Status Close() override { return stream_->Close(); }
  Status Abort() override { return stream_->Abort(); }
  Result<int64_t> Tell() const override { return stream_->Tell(); }
  bool closed() const override { return stream_->closed(); }
  //@}

  //@{
  // @name Readable
  Result<int64_t> Read(int64_t nbytes, void* out) override {
    return stream_->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    return stream_->Read(nbytes);
  }
  const arrow::io::IOContext& io_context() const override {
    return stream_->io_context();
  }
  //@}

  //@{
  // @name InputStream
  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return internal::FromObjectMetadata(metadata_);
  }
  //@}

  //@{
  // @name RandomAccessFile
  Result<int64_t> GetSize() override { return metadata_.size(); }
  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(metadata_.bucket(), metadata_.name(),
                                           gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes, out);
  }
  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    std::shared_ptr<io::InputStream> stream;
    ARROW_ASSIGN_OR_RAISE(stream, factory_(metadata_.bucket(), metadata_.name(),
                                           gcs::Generation(metadata_.generation()),
                                           gcs::ReadFromOffset(position)));
    return stream->Read(nbytes);
  }
  //@}

  // from Seekable
  Status Seek(int64_t position) override {
    ARROW_ASSIGN_OR_RAISE(stream_, factory_(metadata_.bucket(), metadata_.name(),
                                            gcs::Generation(metadata_.generation()),
                                            gcs::ReadFromOffset(position)));
    return Status::OK();
  }

 private:
  InputStreamFactory factory_;
  gcs::ObjectMetadata metadata_;
  std::shared_ptr<io::InputStream> stream_;
};

}  // namespace

google::cloud::Options AsGoogleCloudOptions(const GcsOptions& o) {
  auto options = google::cloud::Options{};
  std::string scheme = o.scheme;
  if (scheme.empty()) scheme = "https";
  if (scheme == "https") {
    options.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeGoogleDefaultCredentials());
  } else {
    options.set<google::cloud::UnifiedCredentialsOption>(
        google::cloud::MakeInsecureCredentials());
  }
  options.set<gcs::UploadBufferSizeOption>(kUploadBufferSize);
  if (!o.endpoint_override.empty()) {
    options.set<gcs::RestEndpointOption>(scheme + "://" + o.endpoint_override);
  }
  return options;
}

class GcsFileSystem::Impl {
 public:
  explicit Impl(GcsOptions o)
      : options_(std::move(o)), client_(AsGoogleCloudOptions(options_)) {}

  const GcsOptions& options() const { return options_; }

  Result<FileInfo> GetFileInfo(const GcsPath& path) {
    if (!path.object.empty()) {
      auto meta = client_.GetObjectMetadata(path.bucket, path.object);
      return GetFileInfoImpl(path, std::move(meta).status(), FileType::File);
    }
    auto meta = client_.GetBucketMetadata(path.bucket);
    return GetFileInfoImpl(path, std::move(meta).status(), FileType::Directory);
  }

  Status DeleteFile(const GcsPath& p) {
    if (!p.object.empty() && p.object.back() == '/') {
      return Status::IOError("The given path (" + p.full_path +
                             ") is a directory, use DeleteDir");
    }
    return internal::ToArrowStatus(client_.DeleteObject(p.bucket, p.object));
  }

  Status CopyFile(const GcsPath& src, const GcsPath& dest) {
    auto metadata =
        client_.RewriteObjectBlocking(src.bucket, src.object, dest.bucket, dest.object);
    return internal::ToArrowStatus(metadata.status());
  }

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const std::string& bucket_name,
                                                           const std::string& object_name,
                                                           gcs::Generation generation,
                                                           gcs::ReadFromOffset offset) {
    auto stream = client_.ReadObject(bucket_name, object_name, generation, offset);
    ARROW_GCS_RETURN_NOT_OK(stream.status());
    return std::make_shared<GcsInputStream>(std::move(stream), bucket_name, object_name,
                                            gcs::Generation(), offset, client_);
  }

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const GcsPath& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
    gcs::EncryptionKey encryption_key;
    ARROW_ASSIGN_OR_RAISE(encryption_key, internal::ToEncryptionKey(metadata));
    gcs::PredefinedAcl predefined_acl;
    ARROW_ASSIGN_OR_RAISE(predefined_acl, internal::ToPredefinedAcl(metadata));
    gcs::KmsKeyName kms_key_name;
    ARROW_ASSIGN_OR_RAISE(kms_key_name, internal::ToKmsKeyName(metadata));
    gcs::WithObjectMetadata with_object_metadata;
    ARROW_ASSIGN_OR_RAISE(with_object_metadata, internal::ToObjectMetadata(metadata));

    auto stream = client_.WriteObject(path.bucket, path.object, encryption_key,
                                      predefined_acl, kms_key_name, with_object_metadata);
    ARROW_GCS_RETURN_NOT_OK(stream.last_status());
    return std::make_shared<GcsOutputStream>(std::move(stream));
  }

  google::cloud::StatusOr<gcs::ObjectMetadata> GetObjectMetadata(const GcsPath& path) {
    return client_.GetObjectMetadata(path.bucket, path.object);
  }

 private:
  static Result<FileInfo> GetFileInfoImpl(const GcsPath& path,
                                          const google::cloud::Status& status,
                                          FileType type) {
    if (status.ok()) {
      return FileInfo(path.full_path, type);
    }
    using ::google::cloud::StatusCode;
    if (status.code() == StatusCode::kNotFound) {
      return FileInfo(path.full_path, FileType::NotFound);
    }
    return internal::ToArrowStatus(status);
  }

  GcsOptions options_;
  gcs::Client client_;
};

bool GcsOptions::Equals(const GcsOptions& other) const {
  return endpoint_override == other.endpoint_override && scheme == other.scheme;
}

std::string GcsFileSystem::type_name() const { return "gcs"; }

bool GcsFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& fs = ::arrow::internal::checked_cast<const GcsFileSystem&>(other);
  return impl_->options().Equals(fs.impl_->options());
}

Result<FileInfo> GcsFileSystem::GetFileInfo(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->GetFileInfo(p);
}

Result<FileInfoVector> GcsFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteDirContents(const std::string& path) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented(
      std::string(__func__) +
      " is not implemented as it is too dangerous to delete all the buckets");
}

Status GcsFileSystem::DeleteFile(const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->DeleteFile(p);
}

Status GcsFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The GCS FileSystem is not fully implemented");
}

Status GcsFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto s, GcsPath::FromString(src));
  ARROW_ASSIGN_OR_RAISE(auto d, GcsPath::FromString(dest));
  return impl_->CopyFile(s, d);
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(),
                                gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::InputStream>> GcsFileSystem::OpenInputStream(
    const FileInfo& info) {
  if (!info.IsFile()) {
    return Status::IOError("Only files can be opened as input streams");
  }
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  return impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(),
                                gcs::ReadFromOffset());
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const std::string& path) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl](const std::string& b, const std::string& o, gcs::Generation g,
                            gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(b, o, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(
      auto stream,
      impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(metadata->generation()),
                             gcs::ReadFromOffset()));

  return std::make_shared<GcsRandomAccessFile>(std::move(open_stream),
                                               *std::move(metadata), std::move(stream));
}

Result<std::shared_ptr<io::RandomAccessFile>> GcsFileSystem::OpenInputFile(
    const FileInfo& info) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(info.path()));
  auto metadata = impl_->GetObjectMetadata(p);
  ARROW_GCS_RETURN_NOT_OK(metadata.status());
  auto impl = impl_;
  auto open_stream = [impl](const std::string& b, const std::string& o, gcs::Generation g,
                            gcs::ReadFromOffset offset) {
    return impl->OpenInputStream(b, o, g, offset);
  };
  ARROW_ASSIGN_OR_RAISE(
      auto stream,
      impl_->OpenInputStream(p.bucket, p.object, gcs::Generation(metadata->generation()),
                             gcs::ReadFromOffset()));

  return std::make_shared<GcsRandomAccessFile>(std::move(open_stream),
                                               *std::move(metadata), std::move(stream));
}

Result<std::shared_ptr<io::OutputStream>> GcsFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  ARROW_ASSIGN_OR_RAISE(auto p, GcsPath::FromString(path));
  return impl_->OpenOutputStream(p, metadata);
}

Result<std::shared_ptr<io::OutputStream>> GcsFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("Append is not supported in GCS");
}

GcsFileSystem::GcsFileSystem(const GcsOptions& options, const io::IOContext& context)
    : FileSystem(context), impl_(std::make_shared<Impl>(options)) {}

namespace internal {

std::shared_ptr<GcsFileSystem> MakeGcsFileSystemForTest(const GcsOptions& options) {
  // Cannot use `std::make_shared<>` as the constructor is private.
  return std::shared_ptr<GcsFileSystem>(
      new GcsFileSystem(options, io::default_io_context()));
}

}  // namespace internal

}  // namespace fs
}  // namespace arrow
