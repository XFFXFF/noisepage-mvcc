
file(GLOB test_srcs ${PROJECT_SOURCE_DIR}/test/*/*test.cpp)

set(CTEST_FLAGS "")
add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} ${CTEST_FLAGS} --verbose)

foreach(test_src ${test_srcs})
    get_filename_component(test_bare_name ${test_src} NAME)
    string(REPLACE ".cpp" "" test_name ${test_bare_name})

    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    add_dependencies(check ${test_name})

    target_link_libraries(${test_name}
        GTest::GTest
        GTest::Main
        noisepage
        ${NOISEPAGE_LINKER_LIBS}
    )
    add_test(${test_name} ${BUILD_OUTPUT_ROOT_DIRECTORY}/${test_name} --gtest_color=yes)
endforeach(test_src ${test_srcs})
