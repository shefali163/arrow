#pragma once

#include <azure/storage/common/storage_credential.hpp>
#include <string>

#include "arrow/filesystem/filesystem.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace fs {

struct ARROW_EXPORT AzureOptions {
  AzureOptions(std::string accountName, std::string accountKey,
               std::string containerName);

  std::string containerName;

  Azure::Storage::StorageSharedKeyCredential storageCred;
};

class ARROW_EXPORT AzureBlobFileSystem : public FileSystem {
 protected:
  class Impl;
  std::shared_ptr<Impl> impl_;
};

}  // namespace fs
}  // namespace arrow