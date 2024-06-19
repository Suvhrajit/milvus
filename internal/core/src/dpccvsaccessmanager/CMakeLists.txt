
# Specify the path to DpcCvsAccessManagerClient.cpp and .h
set(ACCESS_MANAGER_FILES
        DpcCvsAccessManagerClient.cpp
        )

milvus_add_pkg_config("dpccvsaccessmanager")
add_library(dpccvsaccessmanager SHARED ${ACCESS_MANAGER_FILES})

target_link_libraries(
        dpccvsaccessmanager
        milvus_common
        gRPC::grpc++
        gRPC::grpc
        protobuf::libprotobuf
        absl::base)

install(TARGETS dpccvsaccessmanager DESTINATION "${CMAKE_INSTALL_LIBDIR}")

set(CMAKE_VERBOSE_MAKEFILE ON)