#pragma once

#include <memory>
#include <unordered_map>
#include <tuple>
#include <string>
#include <chrono>
#include "ChunkManager.h"
#include "storage/Types.h"
#include <grpcpp/grpcpp.h>
#include "pb/dpc_cvs_access_manager.pb.h"
#include "pb/dpc_cvs_access_manager.grpc.pb.h"
#include "dpccvsaccessmanager/DpcCvsAccessManagerClient.h"

namespace milvus::storage {

class CollectionChunkManager {
public:
    static void Init(const StorageConfig& config);
    static std::shared_ptr<ChunkManager> GetChunkManager(
        const int64_t collection_id,
        const std::string& instance_name,
        bool write_access);
private:
    static StorageConfig storageConfigTemplate;
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> GetDpcCvsAccessManagerClient();
    static std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> dpcCvsAccessManagerClient_;
    static std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> chunkManagerMemoryCache;
    static bool IsExpired(const std::chrono::system_clock::time_point& expiration);
    static StorageConfig GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response);
    static std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> GetNewCredentials(
        salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
        const int64_t collection_id,
        const std::string& instance_name,
        const std::string& bucket_name,
        bool write_access);

    static std::chrono::system_clock::time_point ConvertToChronoTime(const std::string& time_str);
    static std::mutex client_mutex_;
};

} // namespace milvus::storage
