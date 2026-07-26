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

file(GLOB UTIL_SOURCES util/*.c)
file(GLOB SOURCES src/*.c)
file(GLOB HEADERS include/*.h)

add_library(kv_store STATIC ${SOURCES} ${UTIL_SOURCES})

if(UNIX)
    target_link_libraries(kv_store pthread)
endif()

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

add_executable(kv_test src/main.c)
target_link_libraries(kv_test kv_store)