/*
 * table.h
 *
 *  Created on: Jan 13, 2022
 *      Author: mad
 */

#ifndef VNX_ROCKSDB_INCLUDE_TABLE_H_
#define VNX_ROCKSDB_INCLUDE_TABLE_H_

#include <vnx/Type.h>
#include <vnx/Input.hpp>
#include <vnx/Output.hpp>
#include <vnx/Memory.hpp>
#include <vnx/Buffer.hpp>

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/comparator.h>


namespace vnx {
namespace rocksdb {

enum key_mode_e {
	EQUAL,
	GREATER_EQUAL
};

template<typename K, typename V>
class table {
protected:
	struct stream_t {
		vnx::Memory memory;
		vnx::Buffer buffer;
		vnx::MemoryOutputStream stream;
		vnx::TypeOutput out;
		stream_t() : stream(&memory), out(&stream) {}
	};

	class Comparator : public ::rocksdb::Comparator {
	public:
		Comparator() {
			vnx::type<K>().create_dynamic_code(code);
			type_code = vnx::type<K>().get_type_code();
		}

		int Compare(const ::rocksdb::Slice& a, const ::rocksdb::Slice& b) const override
		{
			K lhs;
			K rhs;
			read(a, lhs, type_code, code);
			read(b, rhs, type_code, code);
			if(lhs < rhs) {
				return -1;
			}
			if(rhs < lhs) {
				return 1;
			}
			return 0;
		}

		const char* Name() const override {
			return "vnx.rocksdb.table.Comparator";
		}

		void FindShortestSeparator(std::string* start, const ::rocksdb::Slice& limit) const override {}
		void FindShortSuccessor(std::string* key) const override {}

	private:
		std::vector<uint16_t> code;
		const vnx::TypeCode* type_code = nullptr;
	};

public:
	table() {
		vnx::type<K>().create_dynamic_code(key_code);
		vnx::type<V>().create_dynamic_code(value_code);
		key_type = vnx::type<K>().get_type_code();
		value_type = vnx::type<V>().get_type_code();
	}

	table(const std::string& file_path, const ::rocksdb::Options& options = ::rocksdb::Options())
		:	table()
	{
		open(file_path, options);
	}

	~table() {
		close();
	}

	void open(const std::string& file_path, ::rocksdb::Options options = ::rocksdb::Options())
	{
		close();
		options.comparator = &comparator;
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

	void insert(const K& key, const V& value)
	{
		stream_t key_stream;
		stream_t value_stream;

		::rocksdb::WriteOptions options;
		const auto status = db->Put(options,
				write(key_stream, key, key_type, key_code),
				write(value_stream, value, value_type, value_code));

		if(!status.ok()) {
			throw std::runtime_error("DB::Put() failed with: " + status.ToString());
		}
	}

	bool find(const K& key, V& value) const
	{
		stream_t key_stream;

		::rocksdb::ReadOptions options;
		::rocksdb::PinnableSlice pinned;
		const auto status = db->Get(
				options, db->DefaultColumnFamily(), write(key_stream, key, key_type, key_code), &pinned);

		if(status.IsNotFound()) {
			return false;
		}
		if(!status.ok()) {
			throw std::runtime_error("DB::Get() failed with: " + status.ToString());
		}
		read(pinned, value, value_type, value_code);
		return true;
	}

	void scan(const std::function<void(const K&, const V&)>& callback) const
	{
		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(db->NewIterator(options));

		iter->SeekToFirst();
		while(iter->Valid()) {
			K key = K();
			V value = V();
			read(iter->key(), key, key_type, key_code);
			read(iter->value(), value, value_type, value_code);
			callback(key, value);
			iter->Next();
		}
	}

	bool erase(const K& key)
	{
		stream_t key_stream;

		::rocksdb::WriteOptions options;
		const auto status = db->Delete(options, write(key_stream, key, key_type, key_code));

		if(status.IsNotFound()) {
			return false;
		}
		if(!status.ok()) {
			throw std::runtime_error("DB::Delete() failed with: " + status.ToString());
		}
		return true;
	}

	void truncate()
	{
		::rocksdb::ReadOptions options;
		std::unique_ptr<::rocksdb::Iterator> iter(db->NewIterator(options));

		iter->SeekToFirst();
		while(iter->Valid()) {
			::rocksdb::WriteOptions options;
			const auto status = db->Delete(options, iter->key());
			if(!status.ok()) {
				throw std::runtime_error("DB::Delete() failed with: " + status.ToString());
			}
			iter->Next();
		}
	}

	void flush()
	{
		::rocksdb::FlushOptions options;
		db->Flush(options);
	}

protected:
	template<typename T>
	static void read(const ::rocksdb::Slice& slice, T& value, const vnx::TypeCode* type_code, const std::vector<uint16_t>& code)
	{
		vnx::PointerInputStream stream(slice.data(), slice.size());
		vnx::TypeInput in(&stream);
		vnx::read(in, value, type_code, type_code ? nullptr : code.data());
	}

	template<typename T>
	static ::rocksdb::Slice write(stream_t& stream, const T& value, const vnx::TypeCode* type_code, const std::vector<uint16_t>& code)
	{
		stream.out.reset();
		stream.memory.clear();

		vnx::write(stream.out, value, type_code, type_code ? nullptr : code.data());

		if(stream.memory.get_size()) {
			stream.out.flush();
			stream.buffer = stream.memory;
			return ::rocksdb::Slice((const char*)stream.buffer.data(), stream.buffer.size());
		}
		return ::rocksdb::Slice((const char*)stream.out.get_buffer(), stream.out.get_buffer_pos());
	}

protected:
	::rocksdb::DB* db = nullptr;

	std::vector<uint16_t> key_code;
	std::vector<uint16_t> value_code;

	const vnx::TypeCode* key_type = nullptr;
	const vnx::TypeCode* value_type = nullptr;

	Comparator comparator;

};


} // rocksdb
} // vnx

#endif /* VNX_ROCKSDB_INCLUDE_TABLE_H_ */
