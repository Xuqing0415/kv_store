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

add_definitions(-D_FILE_OFFSET_BITS=64)

include_directories(include)
include_directories(util)

# zstd third-party library
add_subdirectory(third_party/zstd)

file(GLOB UTIL_SOURCES util/*.c)
file(GLOB SOURCES src/*.c)
file(GLOB HEADERS include/*.h)

# Exclude server sources from the library (they depend on platform sockets)
list(FILTER SOURCES EXCLUDE REGEX "src/(resp_server|server_main|chaos_test)\\.c")

add_library(kv_store STATIC ${SOURCES} ${UTIL_SOURCES})
target_link_libraries(kv_store zstd)

if(UNIX)
    target_link_libraries(kv_store pthread)
endif()

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

add_executable(kv_test src/main.c)
target_link_libraries(kv_test kv_store)

# Redis RESP Server
add_executable(kv_server src/server_main.c src/resp_server.c)
target_link_libraries(kv_server kv_store)
if(WIN32)
    target_link_libraries(kv_server ws2_32)
endif()

# Chaos Test
add_executable(kv_chaos src/chaos_test.c)
target_link_libraries(kv_chaos kv_store)