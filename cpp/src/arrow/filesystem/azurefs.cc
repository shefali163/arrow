#include "azurefs.h"

#include <azure/storage/blobs.hpp>
#include <memory>

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

namespace arrow {
using internal::Uri;
namespace fs {

AzureOptions::AzureOptions(std::string accountName, std::string accountKey,
                           std::string containerName, std::string scheme)
    : storageCred(std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
          accountName, accountKey)),
      containerName(std::move(containerName)),
      scheme(scheme) {}

AzureOptions::AzureOptions() {}

Result<AzureOptions> AzureOptions::FromUri(const std::string& uri_string,
                                           const std::string& accountKey) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, accountKey);
}

Result<AzureOptions> AzureOptions::FromUri(const Uri& uri,
                                           const std::string& accountKey) {
  AzureOptions options;
  options.scheme = uri.scheme();
  AZURE_ASSERT(uri.has_host());
  auto host = uri.host();
  AZURE_ASSERT(!uri.username().empty());

  options.containerName = uri.username();

  AZURE_ASSERT(host.find('.') != std::string::npos);
  std::string accountName = host.substr(0, host.find('.'));

  options.storageCred = std::make_shared<Azure::Storage::StorageSharedKeyCredential>(
      accountName, accountKey);

  return options;
}

bool AzureOptions::Equals(const AzureOptions& other) const {
  return containerName == other.containerName && scheme == other.scheme &&
         storageCred == other.storageCred;
}

std::string AzureOptions::getContainerUrl() const {
  return "https://" + storageCred->AccountName + ".blob.core.windows.net/" +
         containerName;
}

std::shared_ptr<const KeyValueMetadata> GetBlobMetadata(
    const Azure::Storage::Blobs::Models::BlobProperties& result) {
  // Dummy value for now
  auto md = std::make_shared<KeyValueMetadata>();
  return md;
}

class AzureBlobFile final : public io::RandomAccessFile {
 public:
  AzureBlobFile(Azure::Storage::Blobs::BlobClient client, const io::IOContext& io_context,
                const std::string& path, int64_t size = kNoSize)
      : client_(client), io_context_(io_context), path_(path), content_length_(size) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }

    auto properties = client_.GetProperties().Value;

    content_length_ = properties.BlobSize;
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
        client_.DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, downloadOptions)
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
  Azure::Storage::Blobs::BlobClient client_;
  const io::IOContext io_context_;
  std::string path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

// -----------------------------------------------------------------------
// Azure filesystem implementation
class AzureBlobFileSystem::Impl
    : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {
 public:
  io::IOContext io_context_;
  std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> client_;

  explicit Impl(AzureOptions options, io::IOContext io_context)
      : io_context_(io_context), options_(std::move(options)) {}

  Status Init() {
    client_ = std::make_shared<Azure::Storage::Blobs::BlobContainerClient>(
        options_.getContainerUrl(), options_.storageCred);
    return Status::OK();
  }

  const AzureOptions& options() const { return options_; }

  Result<std::shared_ptr<AzureBlobFile>> OpenInputFile(const std::string& s,
                                                       AzureBlobFileSystem* fs) {
    Azure::Core::Url url(s);
    auto ptr = std::make_shared<AzureBlobFile>(client_->GetBlobClient(url.GetPath()),
                                               fs->io_context(), s);
    RETURN_NOT_OK(ptr->Init());
    return ptr;
  }

 protected:
  AzureOptions options_;
};

AzureBlobFileSystem::AzureBlobFileSystem(const AzureOptions& options,
                                         const io::IOContext& io_context)
    : FileSystem(io_context), impl_(std::make_shared<Impl>(options, io_context)) {
  default_async_is_sync_ = false;
}

AzureBlobFileSystem::~AzureBlobFileSystem() {}

Result<std::shared_ptr<AzureBlobFileSystem>> AzureBlobFileSystem::Make(
    const AzureOptions& options, const io::IOContext& io_context) {
  std::shared_ptr<AzureBlobFileSystem> ptr(new AzureBlobFileSystem(options, io_context));
  RETURN_NOT_OK(ptr->impl_->Init());
  return ptr;
}

bool AzureBlobFileSystem::Equals(const FileSystem& other) const {
  if (this == &other) {
    return true;
  }
  if (other.type_name() != type_name()) {
    return false;
  }
  const auto& fs = ::arrow::internal::checked_cast<const AzureBlobFileSystem&>(other);
  return impl_->options().Equals(fs.impl_->options());
}

FileInfoGenerator AzureBlobFileSystem::GetFileInfoGenerator(const FileSelector& select) {
  throw std::runtime_error("The Azure FileSystem is not fully implemented");
}

Result<FileInfo> AzureBlobFileSystem::GetFileInfo(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<FileInfoVector> AzureBlobFileSystem::GetFileInfo(const FileSelector& select) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::CreateDir(const std::string& path, bool recursive) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::DeleteDir(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::DeleteDirContents(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::DeleteRootDirContents() {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::DeleteFile(const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::Move(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Status AzureBlobFileSystem::CopyFile(const std::string& src, const std::string& dest) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const std::string& path) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::InputStream>> AzureBlobFileSystem::OpenInputStream(
    const FileInfo& info) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const std::string& path) {
  return impl_->OpenInputFile(path, this);
}

Result<std::shared_ptr<io::RandomAccessFile>> AzureBlobFileSystem::OpenInputFile(
    const FileInfo& info) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenOutputStream(
    const std::string& path, const std::shared_ptr<const KeyValueMetadata>& metadata) {
  return Status::NotImplemented("The Azure FileSystem is not fully implemented");
}

Result<std::shared_ptr<io::OutputStream>> AzureBlobFileSystem::OpenAppendStream(
    const std::string&, const std::shared_ptr<const KeyValueMetadata>&) {
  return Status::NotImplemented("Append is not supported in Azure");
}

}  // namespace fs
}  // namespace arrow