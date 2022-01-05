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

#include "arrow/filesystem/azure.h"

#include <azure/storage/blobs.hpp>
#include <azure/storage/files/datalake.hpp>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

#ifdef _WIN32
// Undefine preprocessor macros that interfere with AWS function / method names
#ifdef GetMessage
#undef GetMessage
#endif
#ifdef GetObject
#undef GetObject
#endif
#endif

#include "arrow/util/windows_fixup.h"

#include "arrow/buffer.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/path_util.h"
#include "arrow/filesystem/util_internal.h"
#include "arrow/io/interfaces.h"
#include "arrow/io/memory.h"
#include "arrow/io/util_internal.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/async_generator.h"
#include "arrow/util/atomic_shared_ptr.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/future.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/util/optional.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

static const char kSep = '/';

namespace arrow {
namespace fs {

struct AzurePath {
  std::string full_path;
  std::string container;
  std::string path_to_file;
  std::vector<std::string> path_to_file_parts;

  static Result<AzurePath> FromString(const std::string& s) {
    // https://synapsemladlsgen2.dfs.core.windows.net/synapsemlfs/testdir/testfile.txt
    // container = synapsemlfs
    // account_name = synapsemladlsgen2
    // path_to_file = testdir/testfile.txt
    // path_to_file_parts = [testdir, testfile.txt]

    // Expected input here => s = /synapsemlfs/testdir/testfile.txt

    const auto src = internal::RemoveTrailingSlash(s);
    auto first_sep = src.find_first_of(kSep);
    if (first_sep == 0) {
      return Status::Invalid("Path cannot start with a separator ('", s, "')");
    }
    if (first_sep == std::string::npos) {
      return AzurePath{std::string(src), std::string(src), "", {}};
    }
    AzurePath path;
    path.full_path = std::string(src);
    path.container = std::string(src.substr(0, first_sep));
    path.path_to_file = std::string(src.substr(first_sep + 1));
    path.path_to_file_parts = internal::SplitAbstractPath(path.path_to_file);
    RETURN_NOT_OK(Validate(&path));
    return path;
  }

  static Status Validate(const AzurePath* path) {
    auto result = internal::ValidateAbstractPathParts(path->path_to_file_parts);
    if (!result.ok()) {
      return Status::Invalid(result.message(), " in path ", path->full_path);
    } else {
      return result;
    }
  }

  AzurePath parent() const {
    DCHECK(!path_to_file_parts.empty());
    auto parent = AzurePath{"", container, "", path_to_file_parts};
    parent.path_to_file_parts.pop_back();
    parent.path_to_file = internal::JoinAbstractPath(parent.path_to_file_parts);
    parent.full_path = parent.container + kSep + parent.path_to_file;
    return parent;
  }

  bool has_parent() const { return !path_to_file.empty(); }

  bool empty() const { return container.empty() && path_to_file.empty(); }

  bool operator==(const AzurePath& other) const {
    return container == other.container && path_to_file == other.path_to_file;
  }
};

std::shared_ptr<const KeyValueMetadata> GetBlobMetadata(const Azure::Response<Azure::Storage::Files::DataLake::Models::PathProperties>& result) {
  // Dummy value for now
  auto md = std::make_shared<KeyValueMetadata>();
  return md;
}

class AzureBlobFile final : public io::RandomAccessFile {
 public:
  AzureBlobFile(Azure::Storage::Files::DataLake::DataLakePathClient pathClient, Azure::Storage::Files::DataLake::DataLakeFileClient fileClient, const io::IOContext& io_context,
                const AzurePath& path, int64_t size = kNoSize)
      : pathClient_(pathClient), fileClient_(fileClient), io_context_(io_context), path_(path), content_length_(size) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }

    auto properties = pathClient_.GetProperties();

    content_length_ = properties.Value.FileSize;
    DCHECK_GE(content_length_, 0);

    metadata_ = GetBlobMetadata(properties);
    return Status::OK();
  }

  Status CheckClosed() const {
    if (closed_) {
      return Status::Invalid("Operation on closed stream");
    }
    return Status::OK();
  }

  Status CheckPosition(int64_t position, const char* action) const {
    if (position < 0) {
      return Status::Invalid("Cannot ", action, " from negative position");
    }
    if (position > content_length_) {
      return Status::IOError("Cannot ", action, " past end of file");
    }
    return Status::OK();
  }

  // RandomAccessFile APIs

  Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() override {
    return metadata_;
  }

  Future<std::shared_ptr<const KeyValueMetadata>> ReadMetadataAsync(
      const io::IOContext& io_context) override {
    return metadata_;
  }

  Status Close() override {
    closed_ = true;
    return Status::OK();
  }

  bool closed() const override { return closed_; }

  Result<int64_t> Tell() const override {
    RETURN_NOT_OK(CheckClosed());
    return pos_;
  }

  Result<int64_t> GetSize() override {
    RETURN_NOT_OK(CheckClosed());
    return content_length_;
  }

  Status Seek(int64_t position) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "seek"));

    pos_ = position;
    return Status::OK();
  }

  Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    nbytes = std::min(nbytes, content_length_ - position);
    if (nbytes == 0) {
      return 0;
    }

    // Read the desired range of bytes
    Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
    Azure::Core::Http::HttpRange range;
    range.Offset = position;
    range.Length = nbytes;
    downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto result =
        fileClient_.DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, downloadOptions)
            .Value;
    AZURE_ASSERT(result.ContentRange.Length.HasValue());
    return result.ContentRange.Length.Value();
  }

  Result<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override {
    RETURN_NOT_OK(CheckClosed());
    RETURN_NOT_OK(CheckPosition(position, "read"));

    // No need to allocate more than the remaining number of bytes
    nbytes = std::min(nbytes, content_length_ - position);

    ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(nbytes, io_context_.pool()));
    if (nbytes > 0) {
      ARROW_ASSIGN_OR_RAISE(int64_t bytes_read,
                            ReadAt(position, nbytes, buf->mutable_data()));
      DCHECK_LE(bytes_read, nbytes);
      RETURN_NOT_OK(buf->Resize(bytes_read));
    }
    return std::move(buf);
  }

  Result<int64_t> Read(int64_t nbytes, void* out) override {
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, ReadAt(pos_, nbytes, out));
    pos_ += bytes_read;
    return bytes_read;
  }

  Result<std::shared_ptr<Buffer>> Read(int64_t nbytes) override {
    ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(pos_, nbytes));
    pos_ += buffer->size();
    return std::move(buffer);
  }

 protected:
  Azure::Storage::Files::DataLake::DataLakePathClient pathClient_;
  Azure::Storage::Files::DataLake::DataLakeFileClient fileClient_;
  const io::IOContext io_context_;
  AzurePath path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

std::string AzureOptions::getServiceUrlForGen1() const {
  return "https://" + account_name + ".blob.core.windows.net/";
}

std::string AzureOptions::getServiceUrlForGen2() const {
  return "https://" + account_name + ".dfs.core.windows.net/";
}

// -----------------------------------------------------------------------
// Azure filesystem implementation
class AzureBlobFileSystem::Impl : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {
 public:
  io::IOContext io_context_;
  std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> gen1Client_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> gen2Client_;

  explicit Impl(AzureOptions options, io::IOContext io_context) : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    gen1Client_ = std::make_shared<Azure::Storage::Blobs::BlobServiceClient>(options_.getServiceUrlForGen1(), options_.storageCred);
    gen2Client_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeServiceClient>(options_.getServiceUrlForGen2(), options_.storageCred);
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

  Status CreateContainer(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    fileSystemClient.CreateIfNotExists();
    return Status::OK();
  }  

  Result<bool> ContainerExists(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    try {
      auto properties = fileSystemClient.GetProperties();
      return true;
    } catch (int e){
      return false;
    }
    // auto filesystems = gen2Client_.ListFileSystems();
    // for (auto filesystem : filesystems.FileSystems) {
    //   if (filesystem.Name == container) {
    //     return true;
    //   }
    // }
    // return false;
  }

  Result<bool> FileExists(const std::string& s) {
    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(s, options_.storageCred);
    try {
      auto properties = pathClient_->GetProperties();
      return true;
    } catch (int e) {
      return false;
    }
    // auto filesystems = gen2Client_.ListFileSystems();
    // for (auto filesystem : filesystems.FileSystems) {
    //   if (filesystem.Name == container) {
    //     return true;
    //   }
    // }
    // return false;
  }

  Status CreateEmptyDir(const std::string& container, std::vector<std::string>& path) {
    auto directoryClient = gen2Client_->GetFileSystemClient(container).GetDirectoryClient(path.front());
    std::vector<std::string>::iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    directoryClient.CreateIfNotExists();
    return Status::OK();
  }

  Status IsNonEmptyDirectory(const AzurePath& path, bool* out) {
    ARROW_ASSIGN_OR_RAISE(bool exists, ContainerExists(path.container));
    if (exists) {
      *out = false;
      return Status::OK();
    }
    auto fileSystemClient = gen2Client_->GetFileSystemClient(path.container);

    int count = 0;
    for (auto file_path : fileSystemClient.ListPaths(true).Paths) {
      if (file_path.Name.rfind(path.path_to_file, 0) == 0) {
        count++;
      }
      if (count >= 2) {
        *out = true;
        return Status::OK();
      }
    }
    if (count == 0) {
      *out = false;
      return Status::OK();
    }
    *out = false;
    return Status::OK();
  }

  Status IsEmptyDirectory(const AzurePath& path, bool* out) {
    ARROW_ASSIGN_OR_RAISE(bool exists, ContainerExists(path.container));
    if (exists) {
      *out = false;
      return Status::OK();
    }

    auto fileSystemClient = gen2Client_->GetFileSystemClient(path.container);
    int count = 0;
    for (auto file_path : fileSystemClient.ListPaths(true).Paths) {
      if (file_path.Name == path.path_to_file) {
        count++;
      }
      if (count > 1) {
        *out = false;
        return Status::OK();
      }
    }
    if (count == 0) {
      *out = false;
      return Status::OK();
    }
    *out = true;
    return Status::OK();
  }

  Status DeleteContainer(const std::string& container) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    fileSystemClient.DeleteIfExists();
    return Status::OK();
  }

  Status DeleteDir(const std::string& container, std::vector<std::string>& path) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
    std::vector<std::string>::iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    directoryClient.DeleteRecursiveIfExists();
    return Status::OK();
  }

  Status DeleteFile(const std::string& container, std::vector<std::string>& path) {
    auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
    if (path.size() == 1) {
      auto fileClient = fileSystemClient.GetFileClient(path.front());
      fileClient.DeleteIfExists();
      return Status::OK();
    }
    std::string file_name = path.back();
    path.pop_back();
    auto directoryClient = fileSystemClient.GetDirectoryClient(path.front());
    std::vector<std::string>::iterator it = path.begin();
    std::advance(it, 1);
    while (it != path.end()) {
      directoryClient = directoryClient.GetSubdirectoryClient(*it);
      ++it;
    }
    auto fileClient = directoryClient.GetFileClient(file_name);
    fileClient.DeleteIfExists();
    return Status::OK();
  }

  Status CopyFile(const AzurePath& src, const AzurePath& dest) {
    //TODO
    // auto fileSystemClient = gen2Client_->GetFileSystemClient(container);
  }

  Result<std::shared_ptr<AzureBlobFile>> OpenInputFile(const std::string& s, AzureBlobFileSystem* fs) {
    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(s, options_.storageCred);
    auto fileClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(s, options_.storageCred);
    auto ptr = std::make_shared<AzureBlobFile>(*pathClient_, *fileClient_, fs->io_context(), path);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

  Result<std::shared_ptr<AzureBlobFile>> OpenInputFile(const FileInfo& info,
                                                         AzureBlobFileSystem* fs) {
    if (info.type() == FileType::NotFound) {
      return ::arrow::fs::internal::PathNotFound(info.path());
    }
    if (info.type() != FileType::File && info.type() != FileType::Unknown) {
      return ::arrow::fs::internal::NotAFile(info.path());
    }

    ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(info.path()));

    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(info.path(), options_.storageCred);
    auto fileClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(info.path(), options_.storageCred);
    auto ptr = std::make_shared<AzureBlobFile>(*pathClient_, *fileClient_, fs->io_context(), path, info.size());
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

 protected:
  AzureOptions options_;
};

AzureBlobFileSystem::AzureBlobFileSystem(const AzureOptions& options, const io::IOContext& io_context) : FileSystem(io_context), impl_(std::make_shared<Impl>(options, io_context)) {
  default_async_is_sync_ = false;
}

AzureBlobFileSystem::~AzureBlobFileSystem() {}

Result<std::shared_ptr<AzureBlobFileSystem>> AzureBlobFileSystem::Make(const AzureOptions& options, const io::IOContext& io_context) {
//   std::shared_ptr<AzureBlobFileSystem> ptr(new AzureBlobFileSystem(options, io_context));
//   RETURN_NOT_OK(ptr->impl_->Init());
//   return ptr;
    return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

bool AzureBlobFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& azurefs = ::arrow::internal::checked_cast<const AzureBlobFileSystem&>(other);
  return options().Equals(azurefs.options());
}

AzureOptions AzureBlobFileSystem::options() const { return impl_->options(); }

Result<FileInfo> AzureBlobFileSystem::GetFileInfo(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));
  FileInfo info;
  info.set_path(s);

  if (path.empty()) {
    // It's the root path ""
    info.set_type(FileType::Directory);
    return info;
  } else if (path.path_to_file.empty()) {
    // It's a container
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      info.set_type(FileType::NotFound);
      return info;
    }
    info.set_type(FileType::Directory);
    return info;
  } else {
    // It's an object
    ARROW_ASSIGN_OR_RAISE(bool file_exists, impl_->FileExists(s));
    if (file_exists) {
      // "File" object found
      //TODO{
      // ARROW_ASSIGN_OR_RAISE(auto properties, impl_->GetFileProperties(path.container));
      // FileObjectToInfo(properties, &info);
      //}
      return info;
    }
    // Not found => perhaps it's an empty "directory"
    bool is_dir = false;
    RETURN_NOT_OK(impl_->IsEmptyDirectory(path, &is_dir));
    if (is_dir) {
      info.set_type(FileType::Directory);
      return info;
    }
    // Not found => perhaps it's a non-empty "directory"
    RETURN_NOT_OK(impl_->IsNonEmptyDirectory(path, &is_dir));
    if (is_dir) {
      info.set_type(FileType::Directory);
    } else {
      info.set_type(FileType::NotFound);
    }
    return info;
  }
}

Result<FileInfoVector> AzureBlobFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::CreateDir(const std::string& s, bool recursive) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.path_to_file.empty()) {
    // Create container
    return impl_->CreateContainer(path.container);
  }

  if (recursive) {
    // Ensure container exists
    ARROW_ASSIGN_OR_RAISE(bool container_exists, impl_->ContainerExists(path.container));
    if (!container_exists) {
      RETURN_NOT_OK(impl_->CreateContainer(path.container));
    }
    std::vector<std::string> parent_path_to_file;

    for (const auto& part : path.path_to_file_parts) {
      parent_path_to_file.push_back(part);
      RETURN_NOT_OK(impl_->CreateEmptyDir(path.container, parent_path_to_file));
    }
    return Status::OK();
  } else {
    // Check parent dir exists
    if (path.has_parent()) {
      AzurePath parent_path = path.parent();
      bool exists;
      RETURN_NOT_OK(impl_->IsNonEmptyDirectory(parent_path, &exists));
      if (!exists) {
        RETURN_NOT_OK(impl_->IsEmptyDirectory(parent_path, &exists));
      }
      if (!exists) {
        return Status::IOError("Cannot create directory '", path.full_path,
                               "': parent directory does not exist");
      }
    }

    // XXX Should we check that no non-directory entry exists?
    // Minio does it for us, not sure about other S3 implementations.
    return impl_->CreateEmptyDir(path.container, path.path_to_file_parts);
  }
}

Status AzureBlobFileSystem::DeleteDir(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  if (path.empty()) {
    return Status::NotImplemented("Cannot delete all Azure Containers");
  }
  if (path.path_to_file.empty()) {
    return impl_->DeleteContainer(path.container);
  }
  RETURN_NOT_OK(impl_->DeleteDir(path.container, path.path_to_file_parts));
}

Status AzureBlobFileSystem::DeleteDirContents(const std::string& s) {
  DeleteDir(s);
  CreateDir(s);
  return Status::OK();
}

Status AzureBlobFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("Cannot delete all Azure Containers");
}

Status AzureBlobFileSystem::DeleteFile(const std::string& s) {
  ARROW_ASSIGN_OR_RAISE(auto path, AzurePath::FromString(s));

  RETURN_NOT_OK(impl_->DeleteFile(path.container, path.path_to_file_parts));
}

Status AzureBlobFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  ARROW_ASSIGN_OR_RAISE(auto src_path, AzurePath::FromString(src));
  // RETURN_NOT_OK(ValidateFilePath(src_path));
  ARROW_ASSIGN_OR_RAISE(auto dest_path, AzurePath::FromString(dest));
  // RETURN_NOT_OK(ValidateFilePath(dest_path));

  if (src_path == dest_path) {
    return Status::OK();
  }
  return impl_->CopyFile(src_path, dest_path);
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const std::string& s) {
  return impl_->OpenInputFile(s, this);
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const std::string& s) {
  return impl_->OpenInputFile(s, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const FileInfo& info) {
  return impl_->OpenInputFile(info, this);
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("Append is not supported in Azure");
}
}
}