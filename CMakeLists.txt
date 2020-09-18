cmake_minimum_required(VERSION 3.10)

project(noisepage-mvcc)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Set a default build type if none was specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message("Setting build type to 'RelWithDebInfo' as none was specified.")
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Choose the type of build." FORCE)
  # Set the possible values of build type for cmake-gui
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

include(CTest)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

set(BUILD_SUPPORT_DIR "${CMAKE_SOURCE_DIR}/build-support")

set(CLANG_TOOLS_VERSION "6.0")
find_package(ClangTools)
if ("$ENV{CMAKE_EXPORT_COMPILE_COMMANDS}" STREQUAL "1" OR CLANG_TIDY_FOUND)
  # Generate a Clang compile_commands.json "compilation database" file for use
  # with various development tools, such as Vim's YouCompleteMe plugin.
  # See http://clang.llvm.org/docs/JSONCompilationDatabase.html
  set(CMAKE_EXPORT_COMPILE_COMMANDS 1)
endif()

# set(NOISEPAGE_LINT_QUIET "--quiet")
###########################################################
# "make format" and "make check-format" targets
###########################################################

# runs clang format and updates files in place.
add_custom_target(format ${BUILD_SUPPORT_DIR}/run_clang_format.py
  ${CLANG_FORMAT_BIN}
  ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
  ${CMAKE_CURRENT_SOURCE_DIR}/src --fix ${NOISEPAGE_LINT_QUIET})

# runs clang format and exits with a non-zero exit code if any files need to be reformatted
add_custom_target(check-format ${BUILD_SUPPORT_DIR}/run_clang_format.py
   ${CLANG_FORMAT_BIN}
   ${BUILD_SUPPORT_DIR}/clang_format_exclusions.txt
   ${CMAKE_CURRENT_SOURCE_DIR}/src ${NOISEPAGE_LINT_QUIET})

###########################################################
# "make clang-tidy" and "make check-clang-tidy" targets
###########################################################
if (${CLANG_TIDY_FOUND})
  # runs clang-tidy and attempts to fix any warning automatically
  add_custom_target(clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json 1
  `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cpp | sed -e '/_generated/g'`)
  # runs clang-tidy and exits with a non-zero exit code if any errors are found.
  add_custom_target(check-clang-tidy ${BUILD_SUPPORT_DIR}/run-clang-tidy.sh ${CLANG_TIDY_BIN} ${CMAKE_BINARY_DIR}/compile_commands.json
  0 `find ${CMAKE_CURRENT_SOURCE_DIR}/src -name \\*.cpp |grep -v -F -f ${CMAKE_CURRENT_SOURCE_DIR}/.clang-tidy-ignore | sed -e '/_generated/g'`)

endif()

string (TOLOWER ${CMAKE_BUILD_TYPE} BUILD_SUBDIR_NAME)
set(BUILD_OUTPUT_ROOT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${BUILD_SUBDIR_NAME}")

set(LIBRARY_OUTPUT_PATH "${CMAKE_CURRENT_BINARY_DIR}/lib")
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}")

set(NOISEPAGE_SRC_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/src/include)
set(NOISEPAGE_TEST_INCLUDE_DIR ${PROJECT_SOURCE_DIR}/test/include)
include_directories(${NOISEPAGE_SRC_INCLUDE_DIR})
include_directories(${NOISEPAGE_TEST_INCLUDE_DIR})

include(cmake/Dependencies.cmake)

add_subdirectory(src)
add_subdirectory(third_party/google-test)
add_subdirectory(test)