cmake_minimum_required(VERSION 3.15)
project(KVStore VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Put the compiled binary in build/bin instead of scattered in build/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# Find threading library (needed for std::thread)
find_package(Threads REQUIRED)

# Define the executable and list every .cpp file it needs
add_executable(kvstore
    src/main.cpp
    src/store/kv_store.cpp
    src/network/server.cpp
    src/replication/replication_manager.cpp
    src/network/follower_connector.cpp
    src/persistence/wal.cpp
    src/persistence/snapshot.cpp

)

# Link the threading library
target_link_libraries(kvstore PRIVATE Threads::Threads)

# So #include "store/kv_store.hpp" style paths work cleanly from src/
target_include_directories(kvstore PRIVATE src)