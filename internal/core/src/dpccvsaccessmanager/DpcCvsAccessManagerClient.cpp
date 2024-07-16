#include "DpcCvsAccessManagerClient.h"

namespace milvus::dpccvsaccessmanager {
DpcCvsAccessManagerClient::DpcCvsAccessManagerClient(std::shared_ptr<grpc::Channel> channel)
    : stub_(salesforce::cdp::dpccvsaccessmanager::v1::DpcCvsAccessManager::NewStub(channel)) {}

salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse DpcCvsAccessManagerClient::GetCredentials(
    salesforce::cdp::dpccvsaccessmanager::v1::ApplicationType application_type,
    const std::string& collection_id,
    const std::string& instance_name,
    const std::string& bucket_name,
    bool write_access) {

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsRequest request;
    request.set_application_type(application_type);
    request.set_collection_id(collection_id);
    request.set_instance_name(instance_name);
    request.set_bucket_name(bucket_name);
    request.set_write_access(write_access);

    salesforce::cdp::dpccvsaccessmanager::v1::GetCredentialsResponse response;
    grpc::ClientContext context;

    grpc::Status status = stub_->GetCredentials(&context, request, &response);

    if (status.ok()) {
        return response;
    } else {
        throw std::runtime_error("gRPC call failed: " + status.error_message());
    }
}
}