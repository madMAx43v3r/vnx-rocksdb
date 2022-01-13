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
	multi_table() = default;

	multi_table(const std::string& file_path)
		:	super_t(file_path)
	{
	}

	void open(const std::string& file_path) {
		super_t::open(file_path);
	}

	void close() {
		super_t::close();
	}

	void insert(const K& key, const V& value)
	{
		std::pair<K, I> key_(key, std::numeric_limits<I>::max());

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		iter->SeekForPrev(super_t::write(super_t::key_stream, key_));
		key_.second = 0;

		if(iter->Valid()) {
			super_t::read(iter->key(), key_);
			if(key_.first == key) {
				if(++key_.second == std::numeric_limits<I>::max()) {
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
			super_t::read(iter->key(), key_);
			if(key_.first != key) {
				break;
			}
			values.emplace_back();
			super_t::read(iter->value(), values.back(), super_t::value_stream.type_code, super_t::value_stream.code);
			iter->Next();
		}
		return values.size();
	}

	bool erase(const K& key, const I& index)
	{
		return super_t::erase(std::pair<K, I>(key, index));
	}

	void erase_all(const K& key)
	{
		const std::pair<K, I> begin(key, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		iter->Seek(super_t::write(super_t::key_stream, begin));
		while(iter->Valid()) {
			std::pair<K, I> key_;
			super_t::read(iter->key(), key_);
			if(key_.first != key) {
				break;
			}
			::rocksdb::WriteOptions options;
			const auto status = super_t::db->Delete(options, iter->key());
			if(!status.ok()) {
				throw std::runtime_error("DB::DeleteRange() failed with: " + status.ToString());
			}
			iter->Next();
		}
	}

	void truncate()
	{
		super_t::truncate();
	}

};


} // rocksdb
} // vnx

#endif /* INCLUDE_VNX_ROCKSDB_MULTI_TABLE_H_ */
