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

struct ARROW_EXPORT AzureOptions{
    AzureOptions();

    std::string getServiceUrlForGen1() const;
    std::string getServiceUrlForGen2() const;

    bool Equals(const AzureOptions& other) const;

    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> storageCred;
    std::string scheme;
    std::string account_name;
    std::string account_key;
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

} // namespace fs
} // namespace arrow