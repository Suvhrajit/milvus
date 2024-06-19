#include "DpcCvsAccessManagerClient.h"
#include "log/Log.h"
#include <grpcpp/grpcpp.h>
#include <grpc/grpc.h>
#include <thread>
#include <typeinfo>

namespace milvus::dpccvsaccessmanager {
std::mutex DpcCvsAccessManagerClient::stub_mutex_;

DpcCvsAccessManagerClient::DpcCvsAccessManagerClient() {
    LOG_SEGCORE_INFO_ << "gsriram: Inside DpcCvsAccessManagerClient constructor. ";

    grpc::ChannelArguments channelArgs;
    channelArgs.SetInt(GRPC_ARG_MAX_CONNECTION_AGE_MS, 60000); // 60 seconds
    channelArgs.SetInt(GRPC_ARG_MAX_CONNECTION_IDLE_MS, 60000); // 60 seconds

    // Create the channel with the arguments
    auto channel_ = grpc::CreateCustomChannel("localhost:7049", grpc::InsecureChannelCredentials(), channelArgs);
    stub_ = salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::NewStub(channel_);
    if (!stub_) {
        LOG_SEGCORE_ERROR_ << "gsriram: Failed to create stub.";
        throw std::runtime_error("Failed to create gRPC stub.");
    }
    auto channel_state = channel_->GetState(false);
    const char* state_name = GetGrpcConnectivityStateName(channel_state);
    LOG_SEGCORE_INFO_ << "gsriram: Created channel state inside the constructor: " << state_name;

}

const char* DpcCvsAccessManagerClient::GetGrpcConnectivityStateName(grpc_connectivity_state state) {
    switch (state) {
        case GRPC_CHANNEL_IDLE:
            return "IDLE";
        case GRPC_CHANNEL_CONNECTING:
            return "CONNECTING";
        case GRPC_CHANNEL_READY:
            return "READY";
        case GRPC_CHANNEL_TRANSIENT_FAILURE:
            return "TRANSIENT_FAILURE";
        case GRPC_CHANNEL_SHUTDOWN:
            return "SHUTDOWN";
        default:
            return "UNKNOWN";
    }
}

salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse DpcCvsAccessManagerClient::GetCredentials(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const std::string& collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    //std::lock_guard<std::mutex> lock(stub_mutex_);

    LOG_SEGCORE_INFO_ << "gsriram: Inside DpcCvsAccessManagerClient::GetCredentials";

    // Prepare the request
    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_application_type(application_type);
    request.set_collection_id(collection_id);
    request.set_instance_name(instance_name);
    request.set_bucket_name(bucket_name);
    request.set_write_access(write_access);

    LOG_SEGCORE_INFO_ << "gsriram: Request prepared - Application Type: " << request.application_type()
                      << ", Collection ID: " << request.collection_id()
                      << ", Instance Name: " << request.instance_name()
                      << ", Bucket Name: " << request.bucket_name()
                      << ", Write Access: " << (request.write_access() ? "true" : "false");

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;
    grpc::ClientContext context;



    if (!stub_) {
        LOG_SEGCORE_ERROR_ << "gsriram: Stub is not initialized.";
        throw std::runtime_error("Stub is not initialized");
    }

    LOG_SEGCORE_INFO_ << "gsriram: Sending gRPC request to GetCredentials." << std::flush;

    try {
        // Perform the gRPC call
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(10);
        context.set_deadline(deadline);
        grpc::Status status = stub_->GetCredentials(&context, request, &response);
		LOG_SEGCORE_INFO_ << "gsriram: After GetCredentials." << std::flush;

        if (status.ok()) {
            LOG_SEGCORE_INFO_ << "gsriram: Received response from GetCredentials.";
            LOG_SEGCORE_INFO_ << "gsriram: Response - Access Key ID: " << response.access_key_id()
                              << ", Secret Access Key: [REDACTED]"
                              << ", Session Token: [REDACTED]"
                              << ", Expiration: " << response.expiration_timestamp();
            return response;
        } else {
            LOG_SEGCORE_ERROR_ << "gsriram: gRPC call failed with error: " << status.error_message()
                               << ", error code: " << status.error_code();
            throw std::runtime_error("gRPC call failed: " + status.error_message());
        }
    } catch (const std::exception& e) {
        LOG_SEGCORE_ERROR_ << "gsriram: Exception during gRPC call: " << e.what();
        throw;
    } catch (...) {
        LOG_SEGCORE_ERROR_ << "gsriram: Unknown exception during gRPC call.";
        throw;
    }
}

} // namespace milvus::dpccvsaccessmanager
