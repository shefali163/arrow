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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>
#include <azure/storage/files/datalake.hpp>

#include "arrow/util/uri.h"
#include "arrow/filesystem/test_util.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/logging.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/testing/future_util.h"

namespace arrow {

using internal::Uri;

namespace fs {

class AzureEnvTestMixin : public ::testing::Test {
 public:
  AzureEnvTestMixin() {};

  const std::string& GetAdlsGen2AccountName() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AdlsGen2AccountName.empty()) {
        return AdlsGen2AccountName;
      }
      return std::getenv("ADLS_GEN2_ACCOUNT_NAME");
    }();
    return connectionString;
  }

  const std::string& GetAdlsGen2AccountKey() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AdlsGen2AccountKey.empty()) {
        return AdlsGen2AccountKey;
      }
      return std::getenv("ADLS_GEN2_ACCOUNT_KEY");
    }();
    return connectionString;
  }

  const std::string& GetAdlsGen2ConnectionString() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AdlsGen2ConnectionStringValue.empty()) {
        return AdlsGen2ConnectionStringValue;
      }
      return std::getenv("ADLS_GEN2_CONNECTION_STRING");
    }();
    return connectionString;
  }

  const std::string& GetAdlsGen2SasUrl() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AdlsGen2SasUrl.empty()) {
        return AdlsGen2SasUrl;
      }
      return std::getenv("ADLS_GEN2_SASURL");
    }();
    return connectionString;
  }

  const std::string& GetAadTenantId() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AadTenantIdValue.empty()) {
        return AadTenantIdValue;
      }
      return std::getenv("AAD_TENANT_ID");
    }();
    return connectionString;
  }

  const std::string& GetAadClientId() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AadClientIdValue.empty()) {
        return AadClientIdValue;
      }
      return std::getenv("AAD_CLIENT_ID");
    }();
    return connectionString;
  }

  const std::string& GetAadClientSecret() {
    const static std::string connectionString = [&]() -> std::string {
      if (!AadClientSecretValue.empty()) {
        return AadClientSecretValue;
      }
      return std::getenv("AAD_CLIENT_SECRET");
    }();
    return connectionString;
  }

 private:
  const std::string& AdlsGen2AccountName = std::getenv("ADLS_GEN2_ACCOUNT_NAME");
  const std::string& AdlsGen2AccountKey = std::getenv("ADLS_GEN2_ACCOUNT_KEY");
  const std::string& AdlsGen2ConnectionStringValue = std::getenv("ADLS_GEN2_CONNECTION_STRING");
  const std::string& AdlsGen2SasUrl = std::getenv("ADLS_GEN2_SASURL");
  const std::string& AadTenantIdValue = std::getenv("AAD_TENANT_ID");
  const std::string& AadClientIdValue = std::getenv("AAD_CLIENT_ID");
  const std::string& AadClientSecretValue = std::getenv("AAD_CLIENT_SECRET");
};

TEST_F(AzureEnvTestMixin, FromAccountKey) {
  AzureOptions options;
  options = AzureOptions::FromAccountKey(this->GetAdlsGen2AccountKey(), this->GetAdlsGen2AccountName());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::StorageCredentials);
  ASSERT_NE(options.storage_credentials_provider, nullptr);
}

TEST_F(AzureEnvTestMixin, FromConnectionString) {
  AzureOptions options;
  options = AzureOptions::FromConnectionString(this->GetAdlsGen2ConnectionString());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ConnectionString);
  ASSERT_NE(options.connection_string, "");
}

TEST_F(AzureEnvTestMixin, FromServicePrincipleCredential) {
  AzureOptions options;
  options = AzureOptions::FromServicePrincipleCredential(this->GetAdlsGen2AccountName(), this->GetAadTenantId(), this->GetAadClientId(), this->GetAadClientSecret());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ServicePrincipleCredentials);
  ASSERT_NE(options.service_principle_credentials_provider, nullptr);
}

TEST_F(AzureEnvTestMixin, FromSas) {
  AzureOptions options;
  options = AzureOptions::FromSas(this->GetAdlsGen2SasUrl());
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_NE(options.sas_token, "");
}

TEST(TestAzureFSOptions, FromUri) {
  AzureOptions options;
  Uri uri;

  //Public container
  ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/"));
  ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Anonymous);
  ASSERT_EQ(options.account_url, "https://testcontainer.dfs.core.windows.net/");

  //Sas Token
  ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/?dummy_sas_token"));
  ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
  ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
  ASSERT_EQ(options.account_url, "https://testcontainer.dfs.core.windows.net/");
  ASSERT_EQ(options.sas_token, "?dummy_sas_token");
}

class TestAzureFileSystem : public AzureEnvTestMixin {
 public:
  void SetUp() override { 
    MakeFileSystem();
    {
      auto fileSystemClient = client_->GetFileSystemClient("container");
      fileSystemClient.CreateIfNotExists();
      fileSystemClient = client_->GetFileSystemClient("empty-container");
      fileSystemClient.CreateIfNotExists();
    }
    {
      auto directoryClient = client_->GetFileSystemClient("container").GetDirectoryClient("emptydir");
      directoryClient.CreateIfNotExists();
      directoryClient = client_->GetFileSystemClient("container").GetDirectoryClient("somedir");
      directoryClient.CreateIfNotExists();
      directoryClient = directoryClient.GetSubdirectoryClient("subdir");
      directoryClient.CreateIfNotExists();
      auto fileClient = directoryClient.GetFileClient("subfile");
      fileClient.CreateIfNotExists();
      std::string s = "sub data";
      fileClient.UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&s[0])), s.size());
      fileClient = client_->GetFileSystemClient("container").GetFileClient("somefile");
      fileClient.CreateIfNotExists();
      s = "some data";
      fileClient.UploadFrom(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&s[0])), s.size());
    }
  }

  void MakeFileSystem() {
    const std::string& account_key = GetAdlsGen2AccountKey();
    const std::string& account_name = GetAdlsGen2AccountName();
    options_.ConfigureAccountKeyCredentials(account_name, account_key);
    auto url = options_.account_url;
    client_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeServiceClient>(url, options_.storage_credentials_provider);
    auto result = AzureBlobFileSystem::Make(options_);
    if (!result.ok()) {
      ARROW_LOG(INFO)
          << "AzureFileSystem::Make failed, err msg is "
          << result.status().ToString();
      return;
    }
    fs_ = *result;
  }

  void AssertObjectContents(Azure::Storage::Files::DataLake::DataLakeServiceClient* client, const std::string& container,
                          const std::string& path_to_file, const std::string& expected) {
    auto pathClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakePathClient>(client->GetUrl() + "/"+ container + "/" + path_to_file, options_.storage_credentials_provider);
    auto size = pathClient_->GetProperties().Value.FileSize;
    auto buf = AllocateResizableBuffer(size, fs_->io_context().pool());
    Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
    Azure::Core::Http::HttpRange range;
    range.Offset = 0;
    range.Length = size;
    downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
    auto fileClient_ = std::make_shared<Azure::Storage::Files::DataLake::DataLakeFileClient>(client->GetUrl() + "/"+ container + "/" + path_to_file, options_.storage_credentials_provider);
    auto result = fileClient_->DownloadTo(reinterpret_cast<uint8_t*>(buf->get()->mutable_data()), size, downloadOptions).Value;
    buf->get()->Equals(Buffer(const_cast<uint8_t*>(reinterpret_cast<const uint8_t*>(&expected[0])), expected.size()));
  }

  void TearDown() override {
    auto fileSystemClient = client_->GetFileSystemClient("container");
    fileSystemClient.DeleteIfExists();
    fileSystemClient = client_->GetFileSystemClient("empty-container");
    fileSystemClient.DeleteIfExists();
    fileSystemClient = client_->GetFileSystemClient("new-container");
    fileSystemClient.DeleteIfExists();
    std::this_thread::sleep_for(std::chrono::seconds(30));
  }

 protected:
  AzureOptions options_;
  std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> client_;
  std::shared_ptr<AzureBlobFileSystem> fs_;
};

TEST_F(TestAzureFileSystem, CreateDir) {

  // Existing container
  ASSERT_OK(fs_->CreateDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::Directory);

  // New container
  AssertFileInfo(fs_.get(), "new-container", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("new-container"));
  AssertFileInfo(fs_.get(), "new-container", FileType::Directory);

  // Existing "directory"
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/somedir"));
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);

  AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);
  ASSERT_OK(fs_->CreateDir("container/emptydir"));
  AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);

  // New "directory"
  AssertFileInfo(fs_.get(), "container/newdir", FileType::NotFound);
  ASSERT_OK(fs_->CreateDir("container/newdir"));
  AssertFileInfo(fs_.get(), "container/newdir", FileType::Directory);

  // New "directory", recursive
  ASSERT_OK(fs_->CreateDir("container/newdir/newsub/newsubsub", /*recursive=*/true));
  AssertFileInfo(fs_.get(), "container/newdir/newsub", FileType::Directory);
  AssertFileInfo(fs_.get(), "container/newdir/newsub/newsubsub", FileType::Directory);

  // Existing "file", should fail
  ASSERT_RAISES(IOError, fs_->CreateDir("container/somefile"));
}

TEST_F(TestAzureFileSystem, DeleteDir) {
  FileSelector select;
  select.base_dir = "container";
  std::vector<FileInfo> infos;

  // Empty "directory"
  ASSERT_OK(fs_->DeleteDir("container/emptydir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[1], "container/somefile", FileType::File);

  // Non-empty "directory"
  ASSERT_OK(fs_->DeleteDir("container/somedir"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/somefile", FileType::File);

  // Leaving parent "directory" empty
  ASSERT_OK(fs_->CreateDir("container/newdir/newsub/newsubsub"));
  ASSERT_OK(fs_->DeleteDir("container/newdir/newsub"));
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "container/newdir", FileType::Directory);  // still exists
  AssertFileInfo(infos[1], "container/somefile", FileType::File);

  // Container
  ASSERT_OK(fs_->DeleteDir("container"));
  AssertFileInfo(fs_.get(), "container", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, DeleteFile) {
  // container
  ASSERT_RAISES(IOError, fs_->DeleteFile("container"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("empty-container"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("nonexistent-container"));

  // "File"
  ASSERT_OK(fs_->DeleteFile("container/somefile"));
  AssertFileInfo(fs_.get(), "container/somefile", FileType::NotFound);
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/nonexistent"));

  // "Directory"
  ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir"));
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
}

TEST_F(TestAzureFileSystem, CopyFile) {
  // "File"
  ASSERT_OK(fs_->CopyFile("container/somefile", "container/newfile"));
  AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "container", "newfile", "some data");
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File, 9);  // still exists
  // Overwrite
  ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/newfile"));
  AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "container", "newfile", "sub data");
  // Nonexistent
  ASSERT_RAISES(IOError, fs_->CopyFile("container/nonexistent", "container/newfile2"));
  ASSERT_RAISES(IOError, fs_->CopyFile("nonexistent-container/somefile", "container/newfile2"));
  ASSERT_RAISES(IOError, fs_->CopyFile("container/somefile", "nonexistent-container/newfile2"));
  AssertFileInfo(fs_.get(), "container/newfile2", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, Move) {
  // "File"
  ASSERT_OK(fs_->Move("container/somefile", "container/newfile"));
  AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 9);
  AssertObjectContents(client_.get(), "container", "newfile", "some data");
  // Source was deleted
  AssertFileInfo(fs_.get(), "container/somefile", FileType::NotFound);

  // Overwrite
  ASSERT_OK(fs_->Move("container/somedir/subdir/subfile", "container/newfile"));
  AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 8);
  AssertObjectContents(client_.get(), "container", "newfile", "sub data");
  // Source was deleted
  AssertFileInfo(fs_.get(), "container/somedir/subdir/subfile", FileType::NotFound);

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->Move("container/non-existent", "container/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("nonexistent-container/somefile", "container/newfile2"));
  ASSERT_RAISES(IOError, fs_->Move("container/somefile", "nonexistent-container/newfile2"));
  AssertFileInfo(fs_.get(), "container/newfile2", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoRoot) { AssertFileInfo(fs_.get(), "", FileType::Directory); }

TEST_F(TestAzureFileSystem, GetFileInfoContainer) {
  AssertFileInfo(fs_.get(), "container", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-container", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container", FileType::NotFound);
  // Trailing slashes
  AssertFileInfo(fs_.get(), "container/", FileType::Directory);
  AssertFileInfo(fs_.get(), "empty-container/", FileType::Directory);
  AssertFileInfo(fs_.get(), "nonexistent-container/", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoObject) {
  // "Directories"
  AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "container/somedir/subdir", FileType::Directory, kNoSize);

  // "Files"
  AssertFileInfo(fs_.get(), "container/somefile", FileType::File, 9);
  AssertFileInfo(fs_.get(), "container/somedir/subdir/subfile", FileType::File, 8);

  // Nonexistent
  AssertFileInfo(fs_.get(), "container/emptyd", FileType::NotFound);
  AssertFileInfo(fs_.get(), "container/somed", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-container/somed", FileType::NotFound);

  // Trailing slashes
  AssertFileInfo(fs_.get(), "container/emptydir/", FileType::Directory, kNoSize);
  AssertFileInfo(fs_.get(), "container/somefile/", FileType::File, 9);
  AssertFileInfo(fs_.get(), "container/emptyd/", FileType::NotFound);
  AssertFileInfo(fs_.get(), "non-existent-container/somed/", FileType::NotFound);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelector) {
  FileSelector select;
  std::vector<FileInfo> infos;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 2);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "container", FileType::Directory);
  AssertFileInfo(infos[1], "empty-container", FileType::Directory);

  // Empty container
  select.base_dir = "empty-container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Nonexistent container
  select.base_dir = "nonexistent-container";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;
  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 3);
  AssertFileInfo(infos[0], "container/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[2], "container/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "container/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  // Non-empty "directories"
  select.base_dir = "container/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/somedir/subdir", FileType::Directory);
  select.base_dir = "container/somedir/subdir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 1);
  AssertFileInfo(infos[0], "container/somedir/subdir/subfile", FileType::File, 8);
  // Nonexistent
  select.base_dir = "container/nonexistent";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.allow_not_found = true;
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.allow_not_found = false;

  // Trailing slashes
  select.base_dir = "empty-container/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);
  select.base_dir = "nonexistent-container/";
  ASSERT_RAISES(IOError, fs_->GetFileInfo(select));
  select.base_dir = "container/";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 3);
}

TEST_F(TestAzureFileSystem, GetFileInfoSelectorRecursive) {
  FileSelector select;
  std::vector<FileInfo> infos;
  select.recursive = true;

  // Root dir
  select.base_dir = "";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 7);
  SortInfos(&infos);
  AssertFileInfo(infos[0], "container", FileType::Directory);
  AssertFileInfo(infos[1], "container/emptydir", FileType::Directory);
  AssertFileInfo(infos[2], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[3], "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[4], "container/somedir/subdir/subfile", FileType::File, 8);
  AssertFileInfo(infos[5], "container/somefile", FileType::File, 9);
  AssertFileInfo(infos[6], "empty-container", FileType::Directory);

  // Empty container
  select.base_dir = "empty-container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty container
  select.base_dir = "container";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 5);
  AssertFileInfo(infos[0], "container/emptydir", FileType::Directory);
  AssertFileInfo(infos[1], "container/somedir", FileType::Directory);
  AssertFileInfo(infos[2], "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[3], "container/somedir/subdir/subfile", FileType::File, 8);
  AssertFileInfo(infos[4], "container/somefile", FileType::File, 9);

  // Empty "directory"
  select.base_dir = "container/emptydir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  ASSERT_EQ(infos.size(), 0);

  // Non-empty "directories"
  select.base_dir = "container/somedir";
  ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
  SortInfos(&infos);
  ASSERT_EQ(infos.size(), 2);
  AssertFileInfo(infos[0], "container/somedir/subdir", FileType::Directory);
  AssertFileInfo(infos[1], "container/somedir/subdir/subfile", FileType::File, 8);
}

TEST_F(TestAzureFileSystem, OpenInputStream) {
  std::shared_ptr<io::InputStream> stream;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputStream("nonexistent-container/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/zzzt"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("container/somefile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(2));
  AssertBufferEqual(*buf, "so");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "ta");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(5));
  AssertBufferEqual(*buf, "");

  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenInputStream("container/somedir/subdir/subfile"));
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "sub data");
  ASSERT_OK_AND_ASSIGN(buf, stream->Read(100));
  AssertBufferEqual(*buf, "");
  ASSERT_OK(stream->Close());

  // "Directories"
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/emptydir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container/somedir"));
  ASSERT_RAISES(IOError, fs_->OpenInputStream("container"));
}

TEST_F(TestAzureFileSystem, OpenInputFile) {
  std::shared_ptr<io::RandomAccessFile> file;
  std::shared_ptr<Buffer> buf;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenInputFile("nonexistent-container/somefile"));
  ASSERT_RAISES(IOError, fs_->OpenInputFile("container/zzzt"));

  // "Files"
  ASSERT_OK_AND_ASSIGN(file, fs_->OpenInputFile("container/somefile"));
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_ASSIGN(buf, file->Read(4));
  AssertBufferEqual(*buf, "some");
  ASSERT_OK_AND_EQ(9, file->GetSize());
  ASSERT_OK_AND_EQ(4, file->Tell());

  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(2, 5));
  AssertBufferEqual(*buf, "me da");
  ASSERT_OK_AND_EQ(4, file->Tell());
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(5, 20));
  AssertBufferEqual(*buf, "data");
  ASSERT_OK_AND_ASSIGN(buf, file->ReadAt(9, 20));
  AssertBufferEqual(*buf, "");

  char result[10];
  ASSERT_OK_AND_EQ(5, file->ReadAt(2, 5, &result));
  ASSERT_OK_AND_EQ(4, file->ReadAt(5, 20, &result));
  ASSERT_OK_AND_EQ(0, file->ReadAt(9, 0, &result));

  // Reading past end of file
  ASSERT_RAISES(IOError, file->ReadAt(10, 20));

  ASSERT_OK(file->Seek(5));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "da");
  ASSERT_OK(file->Seek(9));
  ASSERT_OK_AND_ASSIGN(buf, file->Read(2));
  AssertBufferEqual(*buf, "");
  // Seeking past end of file
  ASSERT_RAISES(IOError, file->Seek(10));
}

TEST_F(TestAzureFileSystem, OpenOutputStream) {
  std::shared_ptr<io::OutputStream> stream;

  // Nonexistent
  ASSERT_RAISES(IOError, fs_->OpenOutputStream("nonexistent-container/somefile"));

  // Create new empty file
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile1", "");

  // Create new file with 1 small write
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile2"));
  ASSERT_OK(stream->Write("some data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile2", "some data");

  // Create new file with 3 small writes
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile3"));
  ASSERT_OK(stream->Write("some "));
  ASSERT_OK(stream->Write(""));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile3", "some new data");

  // Create new file with some large writes
  std::string s1, s2, s3, s4, s5, expected;
  s1 = random_string(6000000, /*seed =*/42);  // More than the 5 MB minimum part upload
  s2 = "xxx";
  s3 = random_string(6000000, 43);
  s4 = "zzz";
  s5 = random_string(600000, 44);
  expected = s1 + s2 + s3 + s4 + s5;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile4"));
  for (auto input : {s1, s2, s3, s4, s5}) {
    ASSERT_OK(stream->Write(input));
    // Clobber source contents.  This shouldn't reflect in the data written.
    input.front() = 'x';
    input.back() = 'x';
  }
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile4", expected);

  // Overwrite
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Write("overwritten data"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile1", "overwritten data");

  // Overwrite and make empty
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/newfile1"));
  ASSERT_OK(stream->Close());
  AssertObjectContents(client_.get(), "container", "newfile1", "");
}

TEST_F(TestAzureFileSystem, OpenOutputStreamAbort) {
  std::shared_ptr<io::OutputStream> stream;
  ASSERT_OK_AND_ASSIGN(stream, fs_->OpenOutputStream("container/somefile"));
  ASSERT_OK(stream->Write("new data"));
  ASSERT_OK(stream->Abort());
  ASSERT_EQ(stream->closed(), true);
  AssertObjectContents(client_.get(), "container", "somefile", "some data");
}

}  // namespace fs
}  // namespace arrow