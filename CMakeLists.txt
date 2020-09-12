cmake_minimum_required(VERSION 3.10)

project(noisepage-mvcc)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(NOISEPAGE_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
include_directories(${NOISEPAGE_SRC_INCLUDE_DIR})

include(cmake/Dependencies.cmake)

add_subdirectory(third_party/google-test)
add_subdirectory(test)