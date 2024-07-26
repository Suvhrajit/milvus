#include "CollectionChunkManager.h"
#include "log/Log.h"
#include "storage/Util.h"
#include <sstream>
#include <iomanip>
#include <ctime>
#include "storage/RemoteChunkManagerSingleton.h"
#include "log/Log.h"
namespace milvus::storage {

std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::dpcCvsAccessManagerClient_ = nullptr;

// Static storage config template to be used to create new collection Id chunk managers
StorageConfig CollectionChunkManager::storageConfigTemplate;

// In-memory cache to store collection Id chunk managers with their expiration time
std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> CollectionChunkManager::chunkManagerMemoryCache;

// Needs to be called in order for new chunk managers to be created. Called in storage_c.cpp.
void CollectionChunkManager::Init(const StorageConfig& config) {
    LOG_SEGCORE_INFO_ << "Initializing CollectionChunkManager with config: " << config.ToString();
    storageConfigTemplate = config;
}

// Helper method to help determine is a chunk manager is still valid based on expiration date
bool CollectionChunkManager::IsExpired(const std::chrono::system_clock::time_point& expiration) {
    bool expired = std::chrono::system_clock::now() > expiration;
    LOG_SEGCORE_INFO_ << "Checking if expiration time is expired: " << expired;
    return expired;
}

std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::GetDpcCvsAccessManagerClient() {
    if(dpcCvsAccessManagerClient_ == nullptr) {
        auto channel = grpc::CreateChannel("dpc-cvs-access-manager.milvus.svc.local:7020", grpc::InsecureChannelCredentials()); // TODO: Use the right endpoint
        dpcCvsAccessManagerClient_ = std::make_shared<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient>(channel);
        LOG_SEGCORE_INFO_ << "Created new DpcCvsAccessManagerClient.";
    }
    return dpcCvsAccessManagerClient_;
}

// Helper method to manage the communication with access manager
std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> CollectionChunkManager::GetNewCredentials(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const int64_t collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    auto client = GetDpcCvsAccessManagerClient();

    // Request new credentials
    try {
        auto response = std::make_shared<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>();
        *response = client->GetCredentials(application_type, std::to_string(collection_id), instance_name, bucket_name, write_access);
        LOG_SEGCORE_INFO_ << "Successfully obtained new credentials for collection ID: " << collection_id;
        return response;
    } catch (const std::runtime_error& e) {
        LOG_SEGCORE_ERROR_ << "Error getting new credentials: " << e.what();
    }
    return nullptr;
}

// Helper method to create a new storage config based on static template and the response from access manager
StorageConfig CollectionChunkManager::GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response) {
    StorageConfig updated_config = storageConfigTemplate;

    updated_config.access_key_id = response.access_key_id();
    updated_config.access_key_value = response.secret_access_key();
    updated_config.session_token = response.session_token();
    updated_config.kms_key_id = response.tenant_key_id();

    LOG_SEGCORE_INFO_ << "Updated storage config with new credentials.";
    return updated_config;
}

// Helper method to convert expiration string to std::chrono::system_clock::time_point
std::chrono::system_clock::time_point CollectionChunkManager::ConvertToChronoTime(const std::string& time_str) {
    std::tm tm = {};
    std::istringstream ss(time_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

// The main method that is used to get or create the collection Id chunk manager
// called in the load_index_c.cpp
std::shared_ptr<ChunkManager> CollectionChunkManager::GetChunkManager(
    const int64_t collection_id,
    const std::string& instance_name,
    bool write_access) {

    if (!storageConfigTemplate.byok_enabled) {
        LOG_SEGCORE_INFO_ << "BYOK not enabled, using RemoteChunkManagerSingleton.";
        return milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();
    }

    const std::string& bucket_name = storageConfigTemplate.bucket_name;
    LOG_SEGCORE_INFO_ << "Getting ChunkManager for collection ID: " << collection_id;

    auto cacheObject = chunkManagerMemoryCache.find(collection_id);
    if (cacheObject != chunkManagerMemoryCache.end()) {
        auto [chunk_manager, expiration] = cacheObject->second;
        if (!IsExpired(expiration)) {
            LOG_SEGCORE_INFO_ << "Found valid ChunkManager in cache for collection ID: " << collection_id;
            return chunk_manager;
        } else {
            LOG_SEGCORE_INFO_ << "Cached ChunkManager expired for collection ID: " << collection_id;
        }
    }

    auto credentials = GetNewCredentials(salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType::MILVUS, collection_id, instance_name, bucket_name, write_access);
    if (credentials == nullptr) {
        LOG_SEGCORE_ERROR_ << "Failed to get new credentials for collection ID: " << collection_id;
        return nullptr;
    }

    auto updated_config = GetUpdatedStorageConfig(*credentials);
    LOG_SEGCORE_INFO_ << "Created updated storage config for collection ID: " << collection_id;

    auto chunk_manager = milvus::storage::CreateChunkManager(updated_config);
    std::chrono::system_clock::time_point expiration = ConvertToChronoTime(credentials->expiration_timestamp());

    chunkManagerMemoryCache[collection_id] = std::make_tuple(chunk_manager, expiration);
    LOG_SEGCORE_INFO_ << "Cached new ChunkManager for collection ID: " << collection_id;

    return chunk_manager;
}

} // namespace milvus::storage