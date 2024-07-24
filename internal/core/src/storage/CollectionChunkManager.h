#pragma once

#include <memory>
#include <unordered_map>
#include <tuple>
#include <string>
#include <chrono>
#include "ChunkManager.h"
#include "storage/Types.h"
#include <grpcpp/grpcpp.h>
#include "pb/dpc_cvs_access_manager.grpc.pb.h"
#include "dpccvsaccessmanager/DpcCvsAccessManagerClient.h"

namespace milvus::storage {

class CollectionChunkManager {
public:
    void Init(const StorageConfig& config);
    std::shared_ptr<ChunkManager> GetCollectionIdChunkManager(
        salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
        const std::string& collection_id,
        const std::string& instance_name,
        bool write_access);

private:
    static StorageConfig storageConfigTemplate;
    static std::unordered_map<std::string, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> chunkManagerMemoryCache;

    bool IsExpired(const std::chrono::system_clock::time_point& expiration);
    StorageConfig GetUpdatedStorageConfig(const milvus::dpccvsaccessmanager::GetCredentialsResponse& response);
    std::shared_ptr<milvus::dpccvsaccessmanager::GetCredentialsResponse> GetNewCredentials(
        salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
        const std::string& collection_id,
        const std::string& instance_name,
        const std::string& bucket_name,
        bool write_access);

    std::chrono::system_clock::time_point ConvertToChronoTime(const std::string& time_str) const;
};

} // namespace milvus::storage
