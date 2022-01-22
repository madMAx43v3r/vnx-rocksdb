cmake_minimum_required(VERSION 3.2)
project(vnx-rocksdb)

set(CMAKE_CXX_STANDARD 11)
if(MSVC)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")
else()
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC -Wall -Wno-unused-function")
endif()

add_compile_options(-DNOGDI)

add_library(vnx_rocksdb SHARED
	src/dummy.cpp
)

target_include_directories(vnx_rocksdb
	PUBLIC
		include
)
target_link_libraries(vnx_rocksdb rocksdb z)

if(MSVC)

	find_library(RocksDB_LIBRARY rocksdb HINTS ${RocksDB_LIBRARIES})
	add_library(rocksdb INTERFACE IMPORTED)
	set_target_properties(rocksdb PROPERTIES INTERFACE_LINK_LIBRARIES "${RocksDB_LIBRARY}")
	
	target_include_directories(vnx_rocksdb PRIVATE ${RocksDB_INCLUDE_DIR})

	find_library(ZLIB_LIBRARY zlib HINTS ${ZLIB_LIBRARIES})
	add_library(z INTERFACE IMPORTED)
	set_target_properties(z PROPERTIES INTERFACE_LINK_LIBRARIES "${ZLIB_LIBRARY}")
	
	target_include_directories(vnx_rocksdb PRIVATE ${ZLIB_INCLUDE_DIR})
	
	set_target_properties(vnx_rocksdb PROPERTIES ENABLE_EXPORTS 1)
	set_target_properties(vnx_rocksdb PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS 1)
	
	target_link_libraries(vnx_rocksdb Rpcrt4 shlwapi)
else()
	target_link_libraries(vnx_rocksdb dl)
endif()



add_executable(test_table test/test_table.cpp)
target_link_libraries(test_table vnx_rocksdb vnx_base)
if(MSVC)
	target_link_libraries(test_table Rpcrt4 shlwapi)

	set_target_properties(test_table PROPERTIES LINK_OPTIONS "/NODEFAULTLIB:LIBCMT")
endif()
target_include_directories(test_table PUBLIC ${VNXBASE_BINARY_DIR})

install(DIRECTORY include/ DESTINATION include)

install(TARGETS vnx_rocksdb DESTINATION lib)
