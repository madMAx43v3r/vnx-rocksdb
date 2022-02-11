cmake_minimum_required(VERSION 2.6)
project(vnx-rocksdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wno-unused-function")

add_compile_options(-DNOGDI)

add_library(vnx_rocksdb SHARED
	src/dummy.cpp
)

target_include_directories(vnx_rocksdb
	PUBLIC
		include
)
target_link_libraries(vnx_rocksdb rocksdb z dl)

add_executable(test_table test/test_table.cpp)
target_link_libraries(test_table vnx_rocksdb vnx_base)

install(DIRECTORY include/ DESTINATION include)

install(TARGETS vnx_rocksdb DESTINATION lib)
