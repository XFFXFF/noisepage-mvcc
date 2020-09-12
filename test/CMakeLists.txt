
file(GLOB test_srcs ${PROJECT_SOURCE_DIR}/test/*/*test.cpp)

foreach(test_src ${test_srcs})
    get_filename_component(test_bare_name ${test_src} NAME)
    string(REPLACE ".cpp" "" test_name ${test_bare_name})
    add_executable(${test_name} EXCLUDE_FROM_ALL ${test_src})
    target_link_libraries(${test_name}
        GTest::GTest
        GTest::Main
        ${NOISEPAGE_LINKER_LIBS}
    )
endforeach(test_src ${test_srcs})
