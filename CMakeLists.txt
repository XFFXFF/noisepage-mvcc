cmake_minimum_required(VERSION 3.10)

project(noisepage-mvcc)

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)
set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")

set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

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