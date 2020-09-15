cmake_minimum_required(VERSION 3.10)

project(noisepage-mvcc)

include(CTest)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(NOISEPAGE_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(NOISEPAGE_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
include_directories(${NOISEPAGE_SRC_INCLUDE_DIR})
include_directories(${NOISEPAGE_TEST_INCLUDE_DIR})

include(cmake/Dependencies.cmake)

add_subdirectory(src)
add_subdirectory(third_party/google-test)
add_subdirectory(test)