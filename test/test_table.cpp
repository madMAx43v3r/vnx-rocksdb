/*
 * test_table.cpp
 *
 *  Created on: Jan 13, 2022
 *      Author: mad
 */

#include <vnx/rocksdb/table.h>
#include <vnx/rocksdb/multi_table.h>

#include <vnx/vnx.h>
#include <vnx/record_index_entry_t.hxx>


int main(int argc, char** argv)
{
	vnx::init("test_table", argc, argv);

	{
		vnx::rocksdb::table<uint64_t, std::string> table("test_table");

		table.insert(1337, "test1");
		table.insert(1338, "test2");
		table.insert(11337, "test3");

		table.erase(1338);

		std::string value;
		if(table.find(1337, value)) {
			std::cout << "value = " << vnx::to_string(value) << std::endl;
		} else {
			std::cout << "value = NOT FOUND" << std::endl;
		}
	}
	{
		vnx::rocksdb::table<uint64_t, vnx::record_index_entry_t> table("test_struct_table");

		vnx::record_index_entry_t value;
		value.pos = 1337;
		table.insert(1337, value);

		if(table.find(1337, value)) {
			std::cout << "value = " << vnx::to_string(value) << std::endl;
		} else {
			std::cout << "value = NOT FOUND" << std::endl;
		}
	}
	{
		vnx::rocksdb::multi_table<uint64_t, std::string> table("test_multi_table");

		table.insert(1337, "test1");
		table.insert(1337, "test2");

		std::vector<std::string> values;
		if(table.find(1337, values)) {
			std::cout << "values = " << vnx::to_string(values) << std::endl;
		} else {
			std::cout << "values = NOT FOUND" << std::endl;
		}
	}

	vnx::close();

	return 0;
}


