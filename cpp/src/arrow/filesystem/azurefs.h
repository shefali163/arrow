#pragma once

#include <azure/storage/common/storage_credential.hpp>
#include <memory>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/macros.h"
#include "arrow/util/uri.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace fs {

struct ARROW_EXPORT AzureOptions {
  AzureOptions(std::string accountName, std::string accountKey, std::string containerName,
               std::string scheme = "abfs");
  AzureOptions();

  static Result<AzureOptions> FromUri(const std::string& uri_string,
                                      const std::string accountKey);
  static Result<AzureOptions> AzureOptions::FromUri(const Uri& uri,
                                                    const std::string accountKey);

  std::string containerName;
  std::string scheme;

  std::unique_ptr<Azure::Storage::StorageSharedKeyCredential> storageCred;
};

class ARROW_EXPORT AzureBlobFileSystem : public FileSystem {
 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow