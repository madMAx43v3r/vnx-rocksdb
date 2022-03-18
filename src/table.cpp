/*
 * table.cpp
 *
 *  Created on: Mar 13, 2022
 *      Author: mad
 */

#include <vnx/rocksdb/table.h>

#include <vnx/vnx.h>


namespace vnx {
namespace rocksdb {

void sync_type_codes(const std::string& file_path)
{
	::rocksdb::Options options;
	options.keep_log_file_num = 3;
	options.OptimizeForSmallDb();

	table<Hash64, TypeCode> table(file_path, options);

	table.scan([](const Hash64& code_hash, const TypeCode& type_code) {
		if(type_code.type_hash) {
			auto copy = std::make_shared<TypeCode>(type_code);
			copy->build();
			vnx::register_type_code(copy);
		}
	});

	for(auto type_code : vnx::get_all_type_codes()) {
		TypeCode tmp;
		if(!table.find(type_code->code_hash, tmp)) {
			table.insert(type_code->code_hash, *type_code);
		}
	}
}


} // rocksdb
} // vnx
