#include "CollectionChunkManager.h"
#include "storage/Util.h"
#include <sstream>
#include <iomanip>
#include <ctime>

namespace milvus::storage {

// static storage config template to be used to create new collection Id chunk managers
StorageConfig CollectionChunkManager::storageConfigTemplate;

// in-memory cache to store collection Id chunk managers with their expiration time
std::unordered_map<std::string, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> CollectionChunkManager::chunkManagerMemoryCache;

// Needs to be called in order for new chunk managers to be created. Called in storage_c.cpp.
void CollectionChunkManager::Init(const StorageConfig& config) {
    storageConfigTemplate = config;
}

// helper method to help determine is a chunk manager is still valid based on expiration date
bool CollectionChunkManager::IsExpired(const std::chrono::system_clock::time_point& expiration) {
    return std::chrono::system_clock::now() > expiration;
}

// helper method to manage the communication with access manager
std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> CollectionChunkManager::GetNewCredentials(
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
    try {
        auto response = std::make_shared<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>();
        *response = client.GetCredentials(application_type, collection_id, instance_name, bucket_name, write_access);
        return response;
    } catch (const std::runtime_error& e) {
        std::cerr << "Error getting new credentials: " << e.what() << std::endl;
    }
    return nullptr;
}

// helper method to create a new storage config based on static template and the response from access manager
StorageConfig CollectionChunkManager::GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response) {
    StorageConfig updated_config = storageConfigTemplate;

    updated_config.access_key_id = response.access_key_id();
    updated_config.access_key_value = response.secret_access_key();
    updated_config.session_token = response.session_token();
    updated_config.expiration_timestamp = response.expiration_timestamp();
    updated_config.kms_key_id = response.tenant_key_id();

    return updated_config;
}

// Helper method to convert expiration string to std::chrono::system_clock::time_point
std::chrono::system_clock::time_point CollectionChunkManager::ConvertToChronoTime(const std::string& time_str) {
    std::tm tm = {};
    std::istringstream ss(time_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

// the main method that is used to get or create the collection Id chunk manager
// called in the load_index_c.cpp
std::shared_ptr<ChunkManager> CollectionChunkManager::GetCollectionIdChunkManager(
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
    if (!response) {
        std::cerr << "Failed to get new credentials for collection ID: " << collection_id << std::endl;
        return nullptr;
    }

    auto updated_config = GetUpdatedStorageConfig(*response);

    auto chunk_manager = milvus::storage::CreateChunkManager(updated_config);

    std::chrono::system_clock::time_point expiration = ConvertToChronoTime(updated_config.expiration_timestamp);

    chunkManagerMemoryCache[collection_id] = std::make_tuple(chunk_manager, expiration);

    return chunk_manager;
}

} // namespace milvus::storage
