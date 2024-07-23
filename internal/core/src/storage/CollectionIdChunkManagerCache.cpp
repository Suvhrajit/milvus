#include "CollectionIdChunkManagerCache.h"
#include <sstream>
#include <iomanip>
#include <ctime>

namespace milvus::storage {

// static storage config template to be used to create new collection Id chunk managers
StorageConfig CollectionIdChunkManagerCache::storageConfigTemplate;

// in-memory cache to store collection Id chunk managers with their expiration time
std::unordered_map<std::string, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> CollectionIdChunkManagerCache::chunkManagerMemoryCache;

// Needs to be called in order for new chunk managers to be created. Called in storage_c.cpp.
void CollectionIdChunkManagerCache::Init(const StorageConfig& config) {
    storageConfigTemplate = config;
}

// helper method to help determine is a chunk manager is still valid based on expiration date
bool CollectionIdChunkManagerCache::IsExpired(const std::chrono::system_clock::time_point& expiration) {
    return std::chrono::system_clock::now() > expiration;
}

// helper method to manage the communication with access manager
std::shared_ptr<milvus::dpccvsaccessmanager::GetCredentialsResponse> CollectionIdChunkManagerCache::GetNewCredentials(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const std::string& collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    // Create a gRPC channel
    auto channel = grpc::CreateChannel("dpc-cvs-access-manager.milvus.svc.local:7020", grpc::InsecureChannelCredentials()); // TODO: Use the correct endpoint

    // Instantiate the client with the channel
    milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient client(channel);

    // Request new credentials
    auto response = std::make_shared<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>();
    *response = client.GetCredentials(application_type, collection_id, instance_name, bucket_name, write_access);

    return response;
}

// helper method to create a new storage config based on static template and the response from access manager
StorageConfig CollectionIdChunkManagerCache::GetUpdatedStorageConfig(const milvus::dpccvsaccessmanager::GetCredentialsResponse& response) {
    StorageConfig updated_config = storageConfigTemplate;

    updated_config.access_key_id = response.access_key_id();
    updated_config.secret_access_key = response.secret_access_key();
    updated_config.session_token = response.session_token();
    updated_config.expiration_timestamp = response.expiration_timestamp();
    updated_config.tenant_key_id = response.tenant_key_id();

    return updated_config;
}

// the main method that is used to get or create the collection Id chunk manager
// called in the load_index_c.cpp
std::shared_ptr<ChunkManager> CollectionIdChunkManagerCache::GetCollectionIdChunkManager(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const std::string& collection_id,
    const std::string& instance_name,
    bool write_access) {

    const std::string& bucket_name = storageConfigTemplate.bucket_name;

    auto cacheObject = chunkManagerMemoryCache.find(collection_id);
    if (cacheObject != chunkManagerMemoryCache.end()) {
        auto [chunk_manager, expiration] = cacheObject->second;
        if (!IsExpired(expiration)) {
            return chunk_manager;
        }
    }

    auto response = GetNewCredentials(application_type, collection_id, instance_name, bucket_name, write_access);
    auto updated_config = GetUpdatedStorageConfig(*response);

    auto chunk_manager = milvus::storage::CreateChunkManager(updated_config);

    // Convert expiration string to std::chrono::system_clock::time_point
    std::tm tm = {};
    std::istringstream ss(updated_config.expiration_timestamp);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    std::chrono::system_clock::time_point expiration = std::chrono::system_clock::from_time_t(std::mktime(&tm));

    chunkManagerMemoryCache[collection_id] = std::make_tuple(chunk_manager, expiration);

    return chunk_manager;
}

} // namespace milvus::storage
