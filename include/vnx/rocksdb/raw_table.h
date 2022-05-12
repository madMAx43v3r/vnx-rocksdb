/*
 * raw_table.h
 *
 *  Created on: May 5, 2022
 *      Author: mad
 */

#ifndef INCLUDE_VNX_ROCKSDB_RAW_TABLE_H_
#define INCLUDE_VNX_ROCKSDB_RAW_TABLE_H_

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>

#include <limits>
#include <atomic>


namespace vnx {
namespace rocksdb {

typedef ::rocksdb::PinnableSlice raw_ptr_t;
typedef std::pair<const void*, size_t> raw_data_t;

class raw_table {
public:
	raw_table() {}

	raw_table(const std::string& file_path, const ::rocksdb::Options& options = ::rocksdb::Options())
		:	raw_table()
	{
		open(file_path, options);
	}

	~raw_table() {
		close();
	}

	void open(const std::string& file_path, ::rocksdb::Options options = ::rocksdb::Options())
	{
		close();
		options.create_if_missing = true;

		const auto status = ::rocksdb::DB::Open(options, file_path, &db);
		if(!status.ok()) {
			throw std::runtime_error("DB::Open() failed with: " + status.ToString());
		}
	}

	void close()
	{
		delete db;
		db = nullptr;
	}

	void insert(const raw_data_t& key, const raw_data_t& value)
	{
		::rocksdb::WriteOptions options;
		const auto status = db->Put(options, to_slice(key), to_slice(value));

		if(!status.ok()) {
			throw std::runtime_error("DB::Put() failed with: " + status.ToString());
		}
	}

	bool find(const raw_data_t& key) const
	{
		raw_ptr_t dummy;
		return find(key, dummy);
	}

	bool find(const raw_data_t& key, raw_ptr_t& value) const
	{
		::rocksdb::ReadOptions options;
		const auto status = db->Get(options, db->DefaultColumnFamily(), to_slice(key), &value);

		if(status.IsNotFound()) {
			return false;
		}
		if(!status.ok()) {
			throw std::runtime_error("DB::Get() failed with: " + status.ToString());
		}
		return true;
	}

	bool find_prev(const raw_data_t& key, raw_ptr_t& value, raw_ptr_t* found_key = nullptr) const
	{
		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(db->NewIterator(options));

		iter->SeekForPrev(to_slice(key));
		if(iter->Valid()) {
			value.PinSelf(iter->value());
			if(found_key) {
				found_key->PinSelf(iter->key());
			}
			return true;
		}
		return false;
	}

	::rocksdb::Iterator* iterator() const
	{
		::rocksdb::ReadOptions options;
		return db->NewIterator(options);
	}

	bool erase(const raw_data_t& key)
	{
		::rocksdb::WriteOptions options;
		const auto status = db->Delete(options, to_slice(key));

		if(status.IsNotFound()) {
			return false;
		}
		if(!status.ok()) {
			throw std::runtime_error("DB::Delete() failed with: " + status.ToString());
		}
		return true;
	}

	size_t erase_many(const std::vector<raw_data_t>& keys)
	{
		std::atomic<size_t> count {0};
		if(keys.size() > size_t(std::numeric_limits<int>::max())) {
			throw std::logic_error("keys.size() > INT_MAX");
		}
#pragma omp parallel for
		for(int i = 0; i < int(keys.size()); ++i) {
			try {
				if(erase(keys[i])) {
					count++;
				}
			} catch(...) {
				// ignore
			}
		}
		return count;
	}

protected:
	static ::rocksdb::Slice to_slice(const raw_data_t& data)
	{
		return ::rocksdb::Slice((const char*)data.first, data.second);
	}

protected:
	::rocksdb::DB* db = nullptr;

};


} // rocksdb
} // vnx

#endif /* INCLUDE_VNX_ROCKSDB_RAW_TABLE_H_ */
