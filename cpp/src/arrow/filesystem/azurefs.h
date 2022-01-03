#pragma once

#include <azure/storage/common/storage_credential.hpp>
#include <memory>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"
#include "arrow/util/visibility.h"

namespace arrow {
using internal::Uri;
namespace fs {

struct ARROW_EXPORT AzureOptions {
  AzureOptions(std::string accountName, std::string accountKey, std::string containerName,
               std::string scheme = "abfs");
  AzureOptions();

  static Result<AzureOptions> FromUri(const std::string& uri_string,
                                      const std::string& accountKey);
  static Result<AzureOptions> FromUri(const Uri& uri, const std::string& accountKey);

  std::string getContainerUrl() const;
  bool Equals(const AzureOptions& other) const;

  std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> storageCred;

  std::string containerName;
  std::string scheme;
};

class ARROW_EXPORT AzureBlobFileSystem : public FileSystem {
 public:
  ~AzureBlobFileSystem() override;

  std::string type_name() const override { return "abfs"; }

  /// Return the original Azure options when constructing the filesystem
  AzureOptions options() const;
  /// Return the actual region this filesystem connects to
  std::string region() const;

  bool Equals(const FileSystem& other) const override;

  /// \cond FALSE
  using FileSystem::GetFileInfo;
  /// \endcond
  Result<FileInfo> GetFileInfo(const std::string& path) override;
  Result<std::vector<FileInfo>> GetFileInfo(const FileSelector& select) override;

  FileInfoGenerator GetFileInfoGenerator(const FileSelector& select) override;

  Status CreateDir(const std::string& path, bool recursive = true) override;

  Status DeleteDir(const std::string& path) override;
  Status DeleteDirContents(const std::string& path) override;
  Status DeleteRootDirContents() override;

  Status DeleteFile(const std::string& path) override;

  Status Move(const std::string& src, const std::string& dest) override;

  Status CopyFile(const std::string& src, const std::string& dest) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(
      const std::string& path) override;

  Result<std::shared_ptr<io::InputStream>> OpenInputStream(const FileInfo& info) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const std::string& path) override;

  Result<std::shared_ptr<io::RandomAccessFile>> OpenInputFile(
      const FileInfo& info) override;

  Result<std::shared_ptr<io::OutputStream>> OpenOutputStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  Result<std::shared_ptr<io::OutputStream>> OpenAppendStream(
      const std::string& path,
      const std::shared_ptr<const KeyValueMetadata>& metadata = {}) override;

  static Result<std::shared_ptr<AzureBlobFileSystem>> Make(
      const AzureOptions& options, const io::IOContext& = io::default_io_context());

 protected:
  explicit AzureBlobFileSystem(const AzureOptions& options, const io::IOContext&);

  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow