#include "azurefs.h"

#include <azure/storage/blobs.hpp>

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
    : storageCred(std::make_unique<Azure::Storage::StorageSharedKeyCredential>(
          accountName, accountKey)),
      containerName(std::move(containerName)),
      scheme(scheme) {}

AzureOptions::AzureOptions() {}

Result<AzureOptions> AzureOptions::FromUri(const std::string& uri_string,
                                           const std::string accountKey) {
  Uri uri;
  RETURN_NOT_OK(uri.Parse(uri_string));
  return FromUri(uri, accountKey);
}

Result<AzureOptions> AzureOptions::FromUri(const Uri& uri, const std::string accountKey) {
  AzureOptions options;
  options.scheme = uri.scheme();

  AZURE_ASSERT(uri.has_host());
  auto host = uri.host();

  auto splitPoint = host.find('@');
  AZURE_ASSERT(splitPoint != std::string::npos);
  options.containerName = host.substr(0, splitPoint);

  AZURE_ASSERT(host.find('.') != std::string::npos);
  std::string accountName = host.substr(splitPoint + 1, host.find('.') - splitPoint);

  options.storageCred = std::make_unique<Azure::Storage::StorageSharedKeyCredential>(
      accountName, accountKey);

  return options;
}

std::shared_ptr<const KeyValueMetadata> GetBlobMetadata(
    const Azure::Storage::Blobs::Models::BlobProperties& result) {
  // Dummy value for now
  auto md = std::make_shared<KeyValueMetadata>();
  return md;
}

class AzureBlobFile final : public io::RandomAccessFile {
 public:
  AzureBlobFile(std::shared_ptr<Azure::Storage::Blobs::BlobClient> client,
                const io::IOContext& io_context, const std::string& path,
                int64_t size = kNoSize)
      : client_(std::move(client)),
        io_context_(io_context),
        path_(path),
        content_length_(size) {}

  Status Init() {
    // Issue a HEAD Object to get the content-length and ensure any
    // errors (e.g. file not found) don't wait until the first Read() call.
    if (content_length_ != kNoSize) {
      DCHECK_GE(content_length_, 0);
      return Status::OK();
    }

    auto properties = client_->GetProperties().Value;

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
    client_ = nullptr;
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
        client_->DownloadTo(reinterpret_cast<uint8_t*>(out), nbytes, downloadOptions)
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
  std::shared_ptr<Azure::Storage::Blobs::BlobClient> client_;
  const io::IOContext io_context_;
  std::string path_;

  bool closed_ = false;
  int64_t pos_ = 0;
  int64_t content_length_ = kNoSize;
  std::shared_ptr<const KeyValueMetadata> metadata_;
};

class AzureBlobFileSystem::Impl
    : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {};
}  // namespace fs
}  // namespace arrow