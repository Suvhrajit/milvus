#include "CollectionChunkManager.h"
#include "log/Log.h"
#include "storage/Util.h"
#include <sstream>
#include <iomanip>
#include <ctime>
#include "storage/RemoteChunkManagerSingleton.h"
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include "log/Log.h"
#include <typeinfo>


namespace milvus::storage {

// Define the static mutex
std::mutex CollectionChunkManager::client_mutex_;
std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::dpcCvsAccessManagerClient_ = nullptr;
StorageConfig CollectionChunkManager::storageConfigTemplate;
std::unordered_map<int64_t, std::tuple<std::shared_ptr<ChunkManager>, std::chrono::system_clock::time_point>> CollectionChunkManager::chunkManagerMemoryCache;

// Needs to be called in order for new chunk managers to be created. Called in storage_c.cpp.
void CollectionChunkManager::Init(const StorageConfig& config) {
    LOG_SEGCORE_INFO_ << "gsriram: Initializing CollectionChunkManager with config: " << config.ToString();
    storageConfigTemplate = config;
}

// Helper method to help determine is a chunk manager is still valid based on expiration date
bool CollectionChunkManager::IsExpired(const std::chrono::system_clock::time_point& expiration) {
    bool expired = std::chrono::system_clock::now() > expiration;
    LOG_SEGCORE_INFO_ << "gsriram: Checking if expiration time is expired: " << expired;
    return expired;
}


std::shared_ptr<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient> CollectionChunkManager::GetDpcCvsAccessManagerClient() {
    LOG_SEGCORE_INFO_ << "gsriram: Entering GetDpcCvsAccessManagerClient.";

    //std::lock_guard<std::mutex> lock(client_mutex_);

    if (!dpcCvsAccessManagerClient_) {
        LOG_SEGCORE_INFO_ << "gsriram: dpcCvsAccessManagerClient_ is null.";

        try {
            dpcCvsAccessManagerClient_ = std::make_shared<milvus::dpccvsaccessmanager::DpcCvsAccessManagerClient>();
            if (!dpcCvsAccessManagerClient_) {
                LOG_SEGCORE_ERROR_ << "gsriram: Failed to create DpcCvsAccessManagerClient.";
                return nullptr;
            }

            LOG_SEGCORE_INFO_ << "gsriram: Created new DpcCvsAccessManagerClient.";
        } catch (const std::exception& e) {
            LOG_SEGCORE_ERROR_ << "gsriram: Exception creating channel: " << e.what();
        } catch (...) {
            LOG_SEGCORE_ERROR_ << "gsriram: Unknown error creating channel.";
        }
    } else {
        LOG_SEGCORE_INFO_ << "gsriram: dpcCvsAccessManagerClient_ is already initialized.";
    }

    LOG_SEGCORE_INFO_ << "gsriram: Returning dpcCvsAccessManagerClient_";
    return dpcCvsAccessManagerClient_;
}

// Helper method to manage the communication with access manager
std::shared_ptr<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse> CollectionChunkManager::GetNewCredentials(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const int64_t collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    LOG_SEGCORE_INFO_ << "gsriram: Going to GetDpcCvsAccessManagerClient";
    auto client = GetDpcCvsAccessManagerClient();

    LOG_SEGCORE_INFO_ << "gsriram: GetDpcCvsAccessManagerClient came through";
	salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;

    // Request new credentials
    try {
		response = client->GetCredentials(application_type, std::to_string(collection_id), instance_name, bucket_name, write_access);
        LOG_SEGCORE_INFO_ << "gsriram: Successfully obtained new credentials for collection ID: " << collection_id;
        return std::make_shared<salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse>(response);
    } catch (const std::exception& e) {
        LOG_SEGCORE_ERROR_ << "gsriram: Error getting new credentials: " << e.what();
    }
    return nullptr;
}

StorageConfig CollectionChunkManager::GetUpdatedStorageConfig(const salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse& response) {
    StorageConfig updated_config = storageConfigTemplate;

    updated_config.access_key_id = response.access_key_id();
    updated_config.access_key_value = response.secret_access_key();
    updated_config.session_token = response.session_token();
    updated_config.kms_key_id = response.tenant_key_id();

    LOG_SEGCORE_INFO_ << "gsriram: Updated storage config with new credentials.";
    return updated_config;
}

std::chrono::system_clock::time_point CollectionChunkManager::ConvertToChronoTime(const std::string& time_str) {
    std::tm tm = {};
    std::istringstream ss(time_str);
    ss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return std::chrono::system_clock::from_time_t(std::mktime(&tm));
}

std::shared_ptr<ChunkManager> CollectionChunkManager::GetChunkManager(
    const int64_t collection_id,
    const std::string& instance_name,
    bool write_access) {
    if (!storageConfigTemplate.byok_enabled) {
        LOG_SEGCORE_INFO_ << "gsriram: BYOK not enabled, using RemoteChunkManagerSingleton.";
        return milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                .GetRemoteChunkManager();
    }

    const std::string& bucket_name = storageConfigTemplate.bucket_name;
    LOG_SEGCORE_INFO_ << "gsriram: Getting ChunkManager for collection ID: " << collection_id;

    auto cacheObject = chunkManagerMemoryCache.find(collection_id);
    if (cacheObject != chunkManagerMemoryCache.end()) {
        //std::lock_guard<std::mutex> lock(client_mutex_);
        auto [chunk_manager, expiration] = cacheObject->second;
        if (!IsExpired(expiration)) {
            LOG_SEGCORE_INFO_ << "gsriram: Found valid ChunkManager in cache for collection ID: " << collection_id;
            return chunk_manager;
        } else {
            LOG_SEGCORE_INFO_ << "gsriram: Cached ChunkManager expired for collection ID: " << collection_id;
        }
    }

    LOG_SEGCORE_INFO_ << "gsriram: Getting GetNewCredentials for collection ID: " << collection_id;
    auto credentials = GetNewCredentials(salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType::MILVUS, collection_id, instance_name, bucket_name, write_access);
    if (credentials == nullptr) {
        LOG_SEGCORE_ERROR_ << "gsriram: Failed to get new credentials for collection ID: " << collection_id;
        return nullptr;
    }
    LOG_SEGCORE_INFO_ << "gsriram: Got NewCredentials for collection ID: " << collection_id;
    auto updated_config = GetUpdatedStorageConfig(*credentials);
    LOG_SEGCORE_INFO_ << "gsriram: Created updated storage config for collection ID: " << collection_id;

    auto chunk_manager = milvus::storage::CreateChunkManager(updated_config);
    std::chrono::system_clock::time_point expiration = ConvertToChronoTime(credentials->expiration_timestamp());

    chunkManagerMemoryCache[collection_id] = std::make_tuple(chunk_manager, expiration);
    LOG_SEGCORE_INFO_ << "gsriram: Cached new ChunkManager for collection ID: " << collection_id;

    return chunk_manager;
}

} // namespace milvus::storage
