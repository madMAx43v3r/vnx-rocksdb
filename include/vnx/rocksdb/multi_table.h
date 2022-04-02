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

	multi_table(const std::string& file_path, const ::rocksdb::Options& options = ::rocksdb::Options())
		:	super_t(file_path, options)
	{
	}

	void open(const std::string& file_path, const ::rocksdb::Options& options = ::rocksdb::Options()) {
		super_t::open(file_path, options);
	}

	void close() {
		super_t::close();
	}

	void insert(const K& key, const V& value)
	{
		std::pair<K, I> key_(key, std::numeric_limits<I>::max());

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->SeekForPrev(super_t::write(key_stream, key_, super_t::key_type, super_t::key_code));
		key_.second = 0;

		if(iter->Valid()) {
			std::pair<K, I> found;
			super_t::read(iter->key(), found, super_t::key_type, super_t::key_code);
			if(found.first == key) {
				key_.second = found.second + 1;
			}
		}
		if(key_.second == std::numeric_limits<I>::max()) {
			throw std::runtime_error("key space overflow");
		}
		super_t::insert(key_, value);
	}

	size_t find(const K& key, std::vector<V>& values, const key_mode_e mode = EQUAL) const
	{
		values.clear();
		std::pair<K, I> key_(key, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->Seek(super_t::write(key_stream, key_, super_t::key_type, super_t::key_code));
		while(iter->Valid()) {
			super_t::read(iter->key(), key_, super_t::key_type, super_t::key_code);
			if(mode == EQUAL && !(key_.first == key)) {
				break;
			}
			try {
				V tmp = V();
				super_t::read(iter->value(), tmp, super_t::value_type, super_t::value_code);
				values.push_back(std::move(tmp));
			} catch(...) {
				// ignore
			}
			iter->Next();
		}
		return values.size();
	}

	size_t find_last(const K& key, std::vector<V>& values, const size_t limit) const
	{
		values.clear();
		std::pair<K, I> key_(key, std::numeric_limits<I>::max());

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->SeekForPrev(super_t::write(key_stream, key_, super_t::key_type, super_t::key_code));

		while(iter->Valid() && values.size() < limit)
		{
			super_t::read(iter->key(), key_, super_t::key_type, super_t::key_code);
			if(!(key_.first == key)) {
				break;
			}
			try {
				V tmp = V();
				super_t::read(iter->value(), tmp, super_t::value_type, super_t::value_code);
				values.push_back(std::move(tmp));
			} catch(...) {
				// ignore
			}
			iter->Prev();
		}
		return values.size();
	}

	size_t find_range(const K& begin, const K& end, std::vector<V>& values) const
	{
		values.clear();
		std::pair<K, I> key_(begin, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->Seek(super_t::write(key_stream, key_, super_t::key_type, super_t::key_code));
		while(iter->Valid()) {
			super_t::read(iter->key(), key_, super_t::key_type, super_t::key_code);
			if(!(key_.first < end)) {
				break;
			}
			try {
				V tmp = V();
				super_t::read(iter->value(), tmp, super_t::value_type, super_t::value_code);
				values.push_back(std::move(tmp));
			} catch(...) {
				// ignore
			}
			iter->Next();
		}
		return values.size();
	}

	size_t find_range(const K& begin, const K& end, std::vector<std::pair<K, V>>& result) const
	{
		result.clear();
		std::pair<K, I> key_(begin, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->Seek(super_t::write(key_stream, key_, super_t::key_type, super_t::key_code));
		while(iter->Valid()) {
			super_t::read(iter->key(), key_, super_t::key_type, super_t::key_code);
			if(!(key_.first < end)) {
				break;
			}
			try {
				std::pair<K, V> tmp;
				tmp.first = key_.first;
				super_t::read(iter->value(), tmp.second, super_t::value_type, super_t::value_code);
				result.push_back(std::move(tmp));
			} catch(...) {
				// ignore
			}
			iter->Next();
		}
		return result.size();
	}

	bool erase(const K& key, const I& index)
	{
		return super_t::erase(std::pair<K, I>(key, index));
	}

	size_t erase_all(const K& key, const key_mode_e mode = EQUAL)
	{
		size_t count = 0;
		const std::pair<K, I> begin(key, 0);

		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(super_t::db->NewIterator(options));

		typename super_t::stream_t key_stream;
		iter->Seek(super_t::write(key_stream, begin, super_t::key_type, super_t::key_code));
		while(iter->Valid()) {
			std::pair<K, I> key_;
			super_t::read(iter->key(), key_, super_t::key_type, super_t::key_code);
			if(mode == EQUAL && !(key_.first == key)) {
				break;
			}
			::rocksdb::WriteOptions options;
			const auto status = super_t::db->Delete(options, iter->key());
			if(!status.ok()) {
				throw std::runtime_error("DB::Delete() failed with: " + status.ToString());
			}
			iter->Next();
			count++;
		}
		return count;
	}

	size_t erase_range(const K& begin, const K& end) const
	{
		std::pair<K, I> begin_(begin, 0);
		std::pair<K, I> end_(end, 0);

		::rocksdb::WriteOptions options;
		typename super_t::stream_t begin_stream;
		typename super_t::stream_t end_stream;
		const auto status = super_t::db->DeleteRange(options, super_t::db->DefaultColumnFamily(),
				super_t::write(begin_stream, begin_, super_t::key_type, super_t::key_code),
				super_t::write(end_stream, end_, super_t::key_type, super_t::key_code));
		if(!status.ok()) {
			throw std::runtime_error("DB::DeleteRange() failed with: " + status.ToString());
		}
		return 0;
	}

	size_t truncate() {
		return super_t::truncate();
	}

	void flush() {
		super_t::flush();
	}

};


} // rocksdb
} // vnx

#endif /* INCLUDE_VNX_ROCKSDB_MULTI_TABLE_H_ */
