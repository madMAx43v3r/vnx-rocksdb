/*
 * multi_table.h
 *
 *  Created on: Jan 13, 2022
 *      Author: mad
 */

#ifndef INCLUDE_VNX_ROCKSDB_MULTI_TABLE_H_
#define INCLUDE_VNX_ROCKSDB_MULTI_TABLE_H_

#include <vnx/rocksdb/table.h>


namespace vnx {
namespace rocksdb {

template<typename K, typename V, typename I = uint32_t>
class multi_table : table<std::pair<K, I>, V> {
private:
	typedef table<std::pair<K, I>, V> super_t;

public:
	multi_table(const std::string& file_path)
		:	super_t(file_path)
	{
	}

	void insert(const K& key, const V& value)
	{
		std::pair<K, I> key_(key, std::numeric_limits<I>::max());

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		iter->SeekForPrev(super_t::write(super_t::key_stream, key_));
		key_.second = 0;

		if(iter->Valid()) {
			super_t::read(iter->key(), key_, super_t::key_stream.code.data());
			if(key_.first == key) {
				if(key_.second++ == std::numeric_limits<I>::max()) {
					throw std::runtime_error("key space overflow");
				}
			}
		}
		super_t::insert(key_, value);
	}

	size_t find(const K& key, std::vector<V>& values) const
	{
		values.clear();
		std::pair<K, I> key_(key, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		iter->Seek(super_t::write(super_t::key_stream, key_));
		while(iter->Valid()) {
			super_t::read(iter->key(), key_, super_t::key_stream.code.data());
			if(key_.first != key) {
				break;
			}
			values.emplace_back();
			super_t::read(iter->value(), values.back(), super_t::value_stream.code.data());
			iter->Next();
		}
		return values.size();
	}

	bool erase(const K& key, const I& index)
	{
		return super_t::erase(std::pair<K, I>(key, index));
	}

};


} // rocksdb
} // vnx

#endif /* INCLUDE_VNX_ROCKSDB_MULTI_TABLE_H_ */
