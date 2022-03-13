cmake_minimum_required(VERSION 3.10)
project(vnx-rocksdb)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_ENABLE_EXPORTS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_WINDOWS_EXPORT_ALL_SYMBOLS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

if(NOT MSVC)
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-function")
endif()

add_compile_definitions(NOGDI NOMINMAX)

find_package(ZLIB REQUIRED)

add_library(vnx_rocksdb SHARED
	src/table.cpp
)

target_include_directories(vnx_rocksdb PUBLIC include)

target_link_libraries(vnx_rocksdb vnx_base rocksdb ZLIB::ZLIB)

if(MSVC)
	find_library(RocksDB_LIBRARY rocksdb HINTS ${RocksDB_LIBRARIES})
	add_library(rocksdb INTERFACE IMPORTED)
	set_target_properties(rocksdb PROPERTIES INTERFACE_LINK_LIBRARIES "${RocksDB_LIBRARY}")

	target_include_directories(vnx_rocksdb PRIVATE ${RocksDB_INCLUDE_DIR})

	target_link_libraries(vnx_rocksdb Rpcrt4 shlwapi)
else()
	target_link_libraries(vnx_rocksdb dl)
endif()

if(VNX_ROCKSDB_BUILD_TESTS)
	add_executable(test_table test/test_table.cpp)
	target_link_libraries(test_table vnx_rocksdb)
	
	if(MSVC)
		set_target_properties(test_table PROPERTIES LINK_OPTIONS "/NODEFAULTLIB:LIBCMT")
	endif()
endif()

install(DIRECTORY include/ DESTINATION include)

install(TARGETS vnx_rocksdb DESTINATION lib)
