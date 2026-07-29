cmake_minimum_required(VERSION 3.15)
project(kv_store C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(MSVC)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /W4 /WX")
else()
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -Werror -O2 -march=native")
endif()

# Static link to avoid runtime DLL dependencies
if(MINGW)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static")
endif()

option(BUILD_TESTS "Build tests" ON)
option(USE_O_DIRECT "Use O_DIRECT to bypass page cache (reduces memory pressure on APU systems)" OFF)

add_definitions(-D_FILE_OFFSET_BITS=64)

# 参数扫描支持：通过 CMake 变量传递编译定义
if(DEFINED KV_MEMTABLE_SIZE_LIMIT)
    add_definitions(-DMEMTABLE_SIZE_LIMIT=${KV_MEMTABLE_SIZE_LIMIT})
endif()
if(USE_O_DIRECT)
    add_definitions(-DUSE_O_DIRECT)
endif()
if(DEFINED KV_SSTABLE_BLOCK_SIZE)
    add_definitions(-DSSTABLE_BLOCK_SIZE=${KV_SSTABLE_BLOCK_SIZE})
endif()
if(DEFINED KV_BLOOM_BITS_PER_KEY)
    add_definitions(-DBLOOM_BITS_PER_KEY=${KV_BLOOM_BITS_PER_KEY})
endif()

include_directories(include)
include_directories(util)

# zstd and lz4 third-party libraries
add_subdirectory(third_party/zstd)
add_subdirectory(third_party/lz4)

file(GLOB UTIL_SOURCES util/*.c)
file(GLOB SOURCES src/*.c)
file(GLOB HEADERS include/*.h)

# Exclude server sources from the library (they depend on platform sockets)
list(FILTER SOURCES EXCLUDE REGEX "src/(resp_server|server_main|chaos_test|metrics_server|raft|raft_node)\\.c")

add_library(kv_store STATIC ${SOURCES} ${UTIL_SOURCES})
target_link_libraries(kv_store zstd lz4)

if(UNIX)
    target_link_libraries(kv_store pthread)
endif()

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

add_executable(kv_test src/main.c)
target_link_libraries(kv_test kv_store)

# Redis RESP Server (with Prometheus metrics)
add_executable(kv_server src/server_main.c src/resp_server.c src/metrics_server.c src/raft.c)
target_link_libraries(kv_server kv_store)
if(WIN32)
    target_link_libraries(kv_server ws2_32)
endif()

# Chaos Test
add_executable(kv_chaos src/chaos_test.c)
target_link_libraries(kv_chaos kv_store)

# Raft Cluster Node (Raft consensus + RESP server + metrics)
add_executable(kv_raft src/raft_node.c src/raft.c src/resp_server.c src/metrics_server.c)
target_link_libraries(kv_raft kv_store)
if(WIN32)
    target_link_libraries(kv_raft ws2_32)
endif()

# Examples
option(BUILD_EXAMPLES "Build examples" ON)
if(BUILD_EXAMPLES)
    add_executable(basic_usage examples/basic_usage.c)
    target_link_libraries(basic_usage kv_store)

    add_executable(backup_restore examples/backup_restore.c)
    target_link_libraries(backup_restore kv_store)

    add_executable(concurrent_example examples/concurrent_example.c)
    target_link_libraries(concurrent_example kv_store)
endif()