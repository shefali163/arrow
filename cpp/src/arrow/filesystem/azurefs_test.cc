// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

// #include "arrow/filesystem/azure.h"

// #include <gtest/gtest.h>
// #include <azure/storage/files/datalake.hpp>

// #include "arrow/util/uri.h"
// #include "arrow/filesystem/test_util.h"
// #include "arrow/testing/gtest_util.h"

// namespace arrow {

// using internal::Uri;

// namespace fs {

// class AzureEnvTestMixin {
//  public:
//   AzureEnvTestMixin() {};

//   const std::string& AdlsGen2AccountName() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AdlsGen2AccountName) != 0) {
//         return AdlsGen2AccountName;
//       }
//       return GetEnv("ADLS_GEN2_ACCOUNT_NAME");
//     }();
//     return connectionString;
//   }

//   const std::string& AdlsGen2AccountKey() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AdlsGen2AccountKey) != 0) {
//         return AdlsGen2AccountKey;
//       }
//       return GetEnv("ADLS_GEN2_ACCOUNT_KEY");
//     }();
//     return connectionString;
//   }

//   const std::string& AdlsGen2ConnectionString() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AdlsGen2ConnectionStringValue) != 0) {
//         return AdlsGen2ConnectionStringValue;
//       }
//       return GetEnv("ADLS_GEN2_CONNECTION_STRING");
//     }();
//     return connectionString;
//   }

//   const std::string& AdlsGen2SasUrl() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AdlsGen2SasUrl) != 0) {
//         return AdlsGen2SasUrl;
//       }
//       return GetEnv("ADLS_GEN2_SASURL");
//     }();
//     return connectionString;
//   }

//   const std::string& AadTenantId() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AadTenantIdValue) != 0) {
//         return AadTenantIdValue;
//       }
//       return GetEnv("AAD_TENANT_ID");
//     }();
//     return connectionString;
//   }

//   const std::string& AadClientId() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AadClientIdValue) != 0) {
//         return AadClientIdValue;
//       }
//       return GetEnv("AAD_CLIENT_ID");
//     }();
//     return connectionString;
//   }

//   const std::string& AadClientSecret() {
//     const static std::string connectionString = [&]() -> std::string {
//       if (strlen(AadClientSecretValue) != 0) {
//         return AadClientSecretValue;
//       }
//       return GetEnv("AAD_CLIENT_SECRET");
//     }();
//     return connectionString;
//   }

//  private:
//   constexpr static const char* AdlsGen2AccountName = "";
//   constexpr static const char* AdlsGen2AccountKey = "";
//   constexpr static const char* AdlsGen2ConnectionStringValue = "";
//   constexpr static const char* AdlsGen2SasUrl = "";
//   constexpr static const char* AadTenantIdValue = "";
//   constexpr static const char* AadClientIdValue = "";
//   constexpr static const char* AadClientSecretValue = "";
// };

// TEST_F(AzureEnvTestMixin, FromAccountKey) {
//   AzureOptions options;
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromAccountKey(this->AdlsGen2AccountKey(), this->AdlsGen2AccountName()));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::StorageCredentials);
//   ASSERT_NE(options.storage_credentials_provider, nullptr);
// }

// TEST_F(AzureEnvTestMixin, FromConnectionString) {
//   AzureOptions options;
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromConnectionString(this->AdlsGen2ConnectionString()));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ConnectionString);
//   ASSERT_NE(options.connection_string, nullptr);
// }

// TEST_F(AzureEnvTestMixin, FromServicePrincipleCredential) {
//   AzureOptions options;
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromServicePrincipleCredential(this->AadTenantId(), this->AadClientId(), this->AadClientSecret()));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::ServicePrincipleCredentials);
//   ASSERT_NE(options.service_principle_credentials_provider, nullptr);
// }

// TEST_F(AzureEnvTestMixin, FromSas) {
//   AzureOptions options;
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromSas(this->AdlsGen2SasUrl()));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
//   ASSERT_NE(options.sas_token, nullptr);
// }

// TEST(TestAzureFSOptions, FromUri) {
//   AzureOptions options;
//   Uri uri;

//   //Public container
//   ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/"));
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Anonymous);
//   ASSERT_EQ(options.account_url, "https://testcontainer.dfs.core.windows.net");

//   //Sas Token
//   ASSERT_OK(uri.Parse("https://testcontainer.dfs.core.windows.net/?dummy_sas_token"));
//   ASSERT_OK_AND_ASSIGN(options, AzureOptions::FromUri(uri));
//   ASSERT_EQ(options.credentials_kind, arrow::fs::AzureCredentialsKind::Sas);
//   ASSERT_EQ(options.account_url, "https://testcontainer.dfs.core.windows.net");
//   ASSERT_EQ(options.sas_token, "?dummy_sas_token");
// }

// class TestAzureFileSystem : public ::testing::Test, public AzureEnvTestMixin {
//  public:
//   void SetUp() override { 
//     MakeFileSystem();
//     // Set up test container and directories
//     {
//       auto fileSystemClient = client_->GetFileSystemClient("container");
//       ASSERT_OK(fileSystemClient.CreateIfNotExists());
//       fileSystemClient = client_->GetFileSystemClient("empty-container");
//       ASSERT_OK(fileSystemClient.CreateIfNotExists());
//     }
//     {
//       auto directoryClient = client_->GetFileSystemClient("container").GetDirectoryClient("emptydir");
//       directoryClient.CreateIfNotExists();
//       directoryClient = client_->GetFileSystemClient("container").GetDirectoryClient("somedir");
//       directoryClient.CreateIfNotExists();
//       directoryClient = directoryClient.GetSubdirectoryClient("subdir");
//       directoryClient.CreateIfNotExists();
//       auto fileClient = directoryClient.GetFileClient("subfile");
//       fileClient.CreateIfNotExists();
//       std::shared_ptr<Buffer>& buffer = "sub data";
//       fileClient.UploadFrom(buffer->data(), buffer->size());
//       fileClient = client_->GetFileSystemClient("container").GetFileClient("somefile");
//       fileClient.CreateIfNotExists();
//       buffer = "some data";
//       fileClient.UploadFrom(buffer->data(), buffer->size());
//     }
//   }

//   void MakeFileSystem() {
//     const char* account_key = AdlsGen2AccountKey();
//     const char* account_name = AdlsGen2AccountName();

//     options_.ConfigureAccountKeyCredentials(account_name, account_key);
//     client_ = std::make_shared<T>(options_.account_url, options_.storage_credentials_provider);
//     auto result = HadoopFileSystem::Make(options_);
//     if (!result.ok()) {
//       ARROW_LOG(INFO)
//           << "AzureFileSystem::Make failed, err msg is "
//           << result.status().ToString();
//       return;
//     }
//     fs_ = *result;
//   }

//   void AssertObjectContents(Azure::Storage::Files::DataLake::DataLakeServiceClient* client, const std::string& container,
//                           const std::string& path_to_file, const std::string& expected) {
//     ARROW_ASSIGN_OR_RAISE(auto buf, AllocateResizableBuffer(reinterpret_cast<int64_t*>(100), fs_->io_context().pool()));
//     Azure::Storage::Blobs::DownloadBlobToOptions downloadOptions;
//     Azure::Core::Http::HttpRange range;
//     range.Offset = 0;
//     range.Length = 100;
//     downloadOptions.Range = Azure::Nullable<Azure::Core::Http::HttpRange>(range);
//     auto result = fileClient_->DownloadTo(reinterpret_cast<uint8_t*>(buf->mutable_data()), 100, downloadOptions).Value;
//     const uint8_t* buf = result->data();
//     for(int i =0 ;i< static_cast<size_t>(buffer_data->size()); i++){
//       std::cout<<buf[i];
//     }
//   }

//  protected:
//   AzureOptions options_;
//   std::shared_ptr<Azure::Storage::Files::DataLake::DataLakeServiceClient> client_;
//   std::shared_ptr<FileSystem> fs_;
// };

// TEST_F(TestAzureFileSystem, CreateDir) {

//   // Existing container
//   ASSERT_OK(fs_->CreateDir("container"));
//   AssertFileInfo(fs_.get(), "container", FileType::Directory);

//   // New container
//   AssertFileInfo(fs_.get(), "new-container", FileType::NotFound);
//   ASSERT_OK(fs_->CreateDir("new-container"));
//   AssertFileInfo(fs_.get(), "new-container", FileType::Directory);

//   // Existing "directory"
//   AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
//   ASSERT_OK(fs_->CreateDir("container/somedir"));
//   AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);

//   AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);
//   ASSERT_OK(fs_->CreateDir("container/emptydir"));
//   AssertFileInfo(fs_.get(), "container/emptydir", FileType::Directory);

//   // New "directory"
//   AssertFileInfo(fs_.get(), "container/newdir", FileType::NotFound);
//   ASSERT_OK(fs_->CreateDir("container/newdir"));
//   AssertFileInfo(fs_.get(), "container/newdir", FileType::Directory);

//   // New "directory", recursive
//   ASSERT_OK(fs_->CreateDir("container/newdir/newsub/newsubsub", /*recursive=*/true));
//   AssertFileInfo(fs_.get(), "container/newdir/newsub", FileType::Directory);
//   AssertFileInfo(fs_.get(), "container/newdir/newsub/newsubsub", FileType::Directory);

//   // Existing "file", should fail
//   ASSERT_RAISES(IOError, fs_->CreateDir("container/somefile"));
// }

// TEST_F(TestAzureFileSystem, DeleteDir) {
//   FileSelector select;
//   select.base_dir = "container";
//   std::vector<FileInfo> infos;

//   // Empty "directory"
//   ASSERT_OK(fs_->DeleteDir("container/emptydir"));
//   ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
//   ASSERT_EQ(infos.size(), 2);
//   SortInfos(&infos);
//   AssertFileInfo(infos[0], "container/somedir", FileType::Directory);
//   AssertFileInfo(infos[1], "container/somefile", FileType::File);

//   // Non-empty "directory"
//   ASSERT_OK(fs_->DeleteDir("container/somedir"));
//   ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
//   ASSERT_EQ(infos.size(), 1);
//   AssertFileInfo(infos[0], "container/somefile", FileType::File);

//   // Leaving parent "directory" empty
//   ASSERT_OK(fs_->CreateDir("container/newdir/newsub/newsubsub"));
//   ASSERT_OK(fs_->DeleteDir("container/newdir/newsub"));
//   ASSERT_OK_AND_ASSIGN(infos, fs_->GetFileInfo(select));
//   ASSERT_EQ(infos.size(), 2);
//   SortInfos(&infos);
//   AssertFileInfo(infos[0], "container/newdir", FileType::Directory);  // still exists
//   AssertFileInfo(infos[1], "container/somefile", FileType::File);

//   // Bucket
//   ASSERT_OK(fs_->DeleteDir("container"));
//   AssertFileInfo(fs_.get(), "container", FileType::NotFound);
// }

// TEST_F(TestAzureFileSystem, DeleteFile) {
//   // container
//   ASSERT_RAISES(IOError, fs_->DeleteFile("container"));
//   ASSERT_RAISES(IOError, fs_->DeleteFile("empty-container"));
//   ASSERT_RAISES(IOError, fs_->DeleteFile("nonexistent-container"));

//   // "File"
//   ASSERT_OK(fs_->DeleteFile("container/somefile"));
//   AssertFileInfo(fs_.get(), "container/somefile", FileType::NotFound);
//   ASSERT_RAISES(IOError, fs_->DeleteFile("container/somefile"));
//   ASSERT_RAISES(IOError, fs_->DeleteFile("container/nonexistent"));

//   // "Directory"
//   ASSERT_RAISES(IOError, fs_->DeleteFile("container/somedir"));
//   AssertFileInfo(fs_.get(), "container/somedir", FileType::Directory);
// }

// TEST_F(TestAzureFileSystem, CopyFile) {
//   // "File"
//   ASSERT_OK(fs_->CopyFile("container/somefile", "container/newfile"));
//   AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 9);
//   AssertObjectContents(client_.get(), "container", "newfile", "some data");
//   AssertFileInfo(fs_.get(), "container/somefile", FileType::File, 9);  // still exists
//   // Overwrite
//   ASSERT_OK(fs_->CopyFile("container/somedir/subdir/subfile", "container/newfile"));
//   AssertFileInfo(fs_.get(), "container/newfile", FileType::File, 8);
//   AssertObjectContents(client_.get(), "container", "newfile", "sub data");
//   // Nonexistent
//   ASSERT_RAISES(IOError, fs_->CopyFile("container/nonexistent", "container/newfile2"));
//   ASSERT_RAISES(IOError, fs_->CopyFile("nonexistent-container/somefile", "container/newfile2"));
//   ASSERT_RAISES(IOError, fs_->CopyFile("container/somefile", "nonexistent-container/newfile2"));
//   AssertFileInfo(fs_.get(), "container/newfile2", FileType::NotFound);
// }

// }  // namespace fs
// }  // namespace arrow