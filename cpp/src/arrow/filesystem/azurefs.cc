#include "azurefs.h"

#include "arrow/io/interfaces.h"

namespace arrow {
namespace fs {
AzureOptions::AzureOptions(std::string accountName, std::string accountKey,
                           std::string containerName)
    : storageCred(accountName, accountKey), containerName(std::move(containerName)) {}

class AzureBlobFile : public io::RandomAccessFile {};

class AzureBlobFileSystem::Impl
    : public std::enable_shared_from_this<AzureBlobFileSystem::Impl> {};
}  // namespace fs
}  // namespace arrow