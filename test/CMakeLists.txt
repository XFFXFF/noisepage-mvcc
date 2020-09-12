
add_executable(concurrent_queue_test ${PROJECT_SOURCE_DIR}/test/common/concurrent_queue_test.cpp)
add_executable(object_pool_test ${PROJECT_SOURCE_DIR}/test/common/object_pool_test.cpp)

target_link_libraries(concurrent_queue_test 
    ${NOISEPAGE_LINKER_LIBS}
    GTest::GTest
    GTest::Main
)

target_link_libraries(object_pool_test 
    ${NOISEPAGE_LINKER_LIBS}
    GTest::GTest
    GTest::Main
)
