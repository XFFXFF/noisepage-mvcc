
file(GLOB srcs ${PROJECT_SOURCE_DIR}/src/*/*.cpp)

add_library(noisepage SHARED ${srcs})

target_link_libraries(noisepage PUBLIC ${NOISEPAGE_LINKER_LIBS})