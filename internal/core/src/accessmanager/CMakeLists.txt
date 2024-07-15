
# Specify the path to access_manager_client.cpp and .h
set(ACCESS_MANAGER_FILES
        access_manager_client.cpp
        )

milvus_add_pkg_config("milvus_accessmanager")
add_library(milvus_accessmanager SHARED ${ACCESS_MANAGER_FILES})

target_link_libraries(milvus_accessmanager milvus_common)

install(TARGETS milvus_accessmanager DESTINATION "${CMAKE_INSTALL_LIBDIR}")

set(CMAKE_VERBOSE_MAKEFILE ON)