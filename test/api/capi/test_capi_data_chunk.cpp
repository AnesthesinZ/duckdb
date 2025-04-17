#include "capi_tester.hpp"

#include <iostream>
#include <duckdb/main/capi/capi_internal.hpp>

namespace duckdb {
struct DuckDBResultData;
}
using namespace duckdb;
using namespace std;

DUCKDB_TYPE schema_v2[11] = {DUCKDB_TYPE_BIGINT,
				  DUCKDB_TYPE_BIGINT,
				  DUCKDB_TYPE_BIGINT,
				  DUCKDB_TYPE_BIGINT,
				  DUCKDB_TYPE_DECIMAL,
				  DUCKDB_TYPE_DECIMAL,
				  DUCKDB_TYPE_DECIMAL,
				  DUCKDB_TYPE_DECIMAL,
				  DUCKDB_TYPE_DATE,
				  DUCKDB_TYPE_DATE,
				  DUCKDB_TYPE_DATE
};

int get_unit(DUCKDB_TYPE type) {
	switch (type) {
	case DUCKDB_TYPE_BIGINT:
	case DUCKDB_TYPE_DECIMAL:
		return sizeof(int64_t);
	case DUCKDB_TYPE_DATE:
		return sizeof(int32_t);
	}
}

duckdb_date create_duckdb_date_v2(int32_t days) {
	duckdb_date date;
	date.days = days;
	return date;
}

void copy_to(int column_count, segment_placeholder* sp, chunk_results* result_ptr, int receive_time, int copy_unit) {
	for(int column_idx = 0; column_idx < column_count; column_idx++) {
		int unit_size = get_unit(schema_v2[column_idx]);
		int seg_idx_to_write_to = sp[column_idx].next_seg_for_write;
		idx_t segment_capacity = sp[column_idx].segment_tuple_counts[seg_idx_to_write_to];

		auto src_ptr = result_ptr->vector_pointers[receive_time * column_count + column_idx];

		if(segment_capacity >= copy_unit) {
			int copy_size = copy_unit;
			// copy all 2048 value to the memory
			auto dest_ptr = sp[column_idx].segment_starts[seg_idx_to_write_to];
			memcpy(dest_ptr, src_ptr, copy_size * unit_size);

			// modify the current segment_capacity
			sp[column_idx].segment_tuple_counts[seg_idx_to_write_to] -= copy_size;
			// advance the pointer.
			sp[column_idx].segment_starts[seg_idx_to_write_to] = dest_ptr +(copy_size * unit_size);
		} else {
			int copy_size = sp[column_idx].segment_tuple_counts[seg_idx_to_write_to];
			sp[column_idx].segment_tuple_counts[seg_idx_to_write_to] -= copy_size;

			// copy segment_capacity amount of value to the memory
			auto dest_ptr = sp[column_idx].segment_starts[seg_idx_to_write_to];
			memcpy(dest_ptr, src_ptr, copy_size * unit_size);

			// advance the seg_idx_to_write_to
			seg_idx_to_write_to += 1;
			sp[column_idx].next_seg_for_write = seg_idx_to_write_to;

			// copy the rest of values to the next seg
			copy_size = copy_unit - copy_size;
			dest_ptr = sp[column_idx].segment_starts[seg_idx_to_write_to];
			memcpy(dest_ptr, src_ptr, copy_size * unit_size);

			// modify the segment_capacity
			sp[column_idx].segment_tuple_counts[seg_idx_to_write_to] -= copy_size;
			// advance the pointer
			sp[column_idx].segment_starts[seg_idx_to_write_to] = dest_ptr +(copy_size * unit_size);
		}
	}
}

TEST_CASE("Test table_info incorrect 'is_valid' value for 'dflt_value' column", "[capi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;

	REQUIRE(duckdb_open("", &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	if(duckdb_query(con, "ATTACH 'tpch-sf1.db' AS file_db;", NULL) != DuckDBSuccess) {
		fprintf(stderr, "Failed to query attach file db.\n");
	}

	if(duckdb_query(con, "ATTACH ':memory:' AS trans_mem_db;", NULL) != DuckDBSuccess) {
		fprintf(stderr, "Failed to query attach mem db.\n");
	}

	// if(duckdb_query(con, "COPY FROM DATABASE file_db TO trans_mem_db;", NULL) != DuckDBSuccess) {
	// 	fprintf(stderr, "Failed to copy to mem_db.\n");
	// }

	REQUIRE(duckdb_query(con, "USE trans_mem_db;", NULL) == DuckDBSuccess);

	if (duckdb_query(con, "CREATE TABLE lineitem_cp ("
												   "l_orderkey BIGINT, l_partkey BIGINT,l_suppkey BIGINT,l_linenumber BIGINT,"
												   "l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), "
												   "l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE"
												   ");", NULL) != DuckDBSuccess ) {
		fprintf(stderr, "failed to create copy table\n");
		exit(-1);
												   }

	// if (duckdb_query(con, "CREATE TABLE lineitem_cp ("
	// 											   "l_orderkey BIGINT"
	// 											   ");", NULL) != DuckDBSuccess ) {
	// 	fprintf(stderr, "failed to create copy table\n");
	// 	exit(-1);
												   // }

	// duckdb_query(con, "SELECT l_orderkey "
			   // "FROM file_db.lineitem LIMIT 204810;", &result);

	duckdb_query(con, "SELECT l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_shipdate,l_commitdate,l_receiptdate "
			   "FROM file_db.lineitem;", &result);

	chunk_results* result_ptr = duckdb_chunk_data_ptrs(result);

	duckdb_appender appender;
	if (duckdb_appender_create(con, NULL, "lineitem_cp", &appender) == DuckDBError) {
		// handle error
		printf("Failed to create appender\n");
		exit(-1);
	}

	const int column_count = 11;
	// size_t target_allocation_size = result_ptr->total_rows;
	size_t target_allocation_size = 6001215 * 3;

	std::cout << "to allocate " << target_allocation_size << " records" << std::endl;
	auto prepareStartTime = std::chrono::high_resolution_clock::now();
	auto sp = duckdb_appender_placeholder(appender, target_allocation_size, column_count);
	auto prepareEndTime = std::chrono::high_resolution_clock::now();

	auto prepareDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
							  prepareEndTime - prepareStartTime)
							  .count();

	std::cout << "Preallocated prepare completed in " << prepareDuration << " milliseconds" << std::endl;
	//
	// size_t receive_times = target_allocation_size / 2048;
	// size_t remainder = target_allocation_size % 2048;
	//
	// int copy_unit = 2048;
	//
	// auto insertStartTime = std::chrono::high_resolution_clock::now();
	// for(int receive_time = 0; receive_time < receive_times; receive_time++) {
	// 	copy_to(column_count, sp, result_ptr, receive_time, copy_unit);
	// }
	//
	// if(remainder > 0) {
	// 	copy_to(column_count, sp, result_ptr, receive_times, remainder);
	// }
	//
	// auto insertEndTime = std::chrono::high_resolution_clock::now();
	//
	// auto insertDuration = std::chrono::duration_cast<std::chrono::milliseconds>(
	// 						  insertEndTime - insertStartTime)
	// 						  .count();
	//
	// std::cout << "insertion completed in " << insertDuration << " milliseconds" << std::endl;

	duckdb_destroy_segment_placeholder(sp, column_count);
	duckdb_appender_destroy(&appender);

	duckdb_destroy_chunk_data_ptrs(result_ptr);
	duckdb_destroy_result(&result);

	// if(duckdb_query(con, "ATTACH 'tt.db' AS transfer;", &result) != DuckDBSuccess) {
	// 	fprintf(stderr, "Failed to attach file db.\n");
	// }
	//
	// if(duckdb_query(con, "DROP TABLE transfer.lineitem_cp;", &result) != DuckDBSuccess) {
	// 	fprintf(stderr, "Failed to drop result table.\n");
	// }
	//
	// if(duckdb_query(con, "COPY FROM DATABASE trans_mem_db TO transfer;", &result) != DuckDBSuccess) {
	// 	fprintf(stderr, "Failed to copy to file db.\n");
	// }

	duckdb_disconnect(&con);
	duckdb_close(&db);

	int z = 1;
}

TEST_CASE("Test Logical Types C API", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_BIGINT);
	duckdb_destroy_logical_type(&type);
	duckdb_destroy_logical_type(&type);

	// list type
	duckdb_logical_type elem_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
	duckdb_logical_type list_type = duckdb_create_list_type(elem_type);
	REQUIRE(duckdb_get_type_id(list_type) == DUCKDB_TYPE_LIST);
	duckdb_logical_type elem_type_dup = duckdb_list_type_child_type(list_type);
	REQUIRE(elem_type_dup != elem_type);
	REQUIRE(duckdb_get_type_id(elem_type_dup) == duckdb_get_type_id(elem_type));
	duckdb_destroy_logical_type(&elem_type);
	duckdb_destroy_logical_type(&list_type);
	duckdb_destroy_logical_type(&elem_type_dup);

	// map type
	duckdb_logical_type key_type = duckdb_create_logical_type(DUCKDB_TYPE_SMALLINT);
	duckdb_logical_type value_type = duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
	duckdb_logical_type map_type = duckdb_create_map_type(key_type, value_type);
	REQUIRE(duckdb_get_type_id(map_type) == DUCKDB_TYPE_MAP);
	duckdb_logical_type key_type_dup = duckdb_map_type_key_type(map_type);
	duckdb_logical_type value_type_dup = duckdb_map_type_value_type(map_type);
	REQUIRE(key_type_dup != key_type);
	REQUIRE(value_type_dup != value_type);
	REQUIRE(duckdb_get_type_id(key_type_dup) == duckdb_get_type_id(key_type));
	REQUIRE(duckdb_get_type_id(value_type_dup) == duckdb_get_type_id(value_type));
	duckdb_destroy_logical_type(&key_type);
	duckdb_destroy_logical_type(&value_type);
	duckdb_destroy_logical_type(&map_type);
	duckdb_destroy_logical_type(&key_type_dup);
	duckdb_destroy_logical_type(&value_type_dup);

	duckdb_destroy_logical_type(nullptr);
}

TEST_CASE("Test DataChunk C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;
	duckdb_state status;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	// create column types
	const idx_t COLUMN_COUNT = 3;
	duckdb_type duckdbTypes[COLUMN_COUNT];
	duckdbTypes[0] = DUCKDB_TYPE_BIGINT;
	duckdbTypes[1] = DUCKDB_TYPE_SMALLINT;
	duckdbTypes[2] = DUCKDB_TYPE_BLOB;

	duckdb_logical_type types[COLUMN_COUNT];
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		types[i] = duckdb_create_logical_type(duckdbTypes[i]);
	}

	// create data chunk
	auto data_chunk = duckdb_create_data_chunk(types, COLUMN_COUNT);
	REQUIRE(data_chunk);
	REQUIRE(duckdb_data_chunk_get_column_count(data_chunk) == COLUMN_COUNT);

	// test duckdb_vector_get_column_type
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto vector = duckdb_data_chunk_get_vector(data_chunk, i);
		auto type = duckdb_vector_get_column_type(vector);
		REQUIRE(duckdb_get_type_id(type) == duckdbTypes[i]);
		duckdb_destroy_logical_type(&type);
	}

	REQUIRE(duckdb_data_chunk_get_vector(data_chunk, 999) == nullptr);
	REQUIRE(duckdb_data_chunk_get_vector(nullptr, 0) == nullptr);
	REQUIRE(duckdb_vector_get_column_type(nullptr) == nullptr);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);
	REQUIRE(duckdb_data_chunk_get_size(nullptr) == 0);

	// create table
	tester.Query("CREATE TABLE test(i BIGINT, j SMALLINT, k BLOB)");

	// use the appender to insert values using the data chunk API
	duckdb_appender appender;
	status = duckdb_appender_create(tester.connection, nullptr, "test", &appender);
	REQUIRE(status == DuckDBSuccess);

	// get the column types from the appender
	REQUIRE(duckdb_appender_column_count(appender) == COLUMN_COUNT);

	// test duckdb_appender_column_type
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto type = duckdb_appender_column_type(appender, i);
		REQUIRE(duckdb_get_type_id(type) == duckdbTypes[i]);
		duckdb_destroy_logical_type(&type);
	}

	// append BIGINT
	auto bigint_vector = duckdb_data_chunk_get_vector(data_chunk, 0);
	auto int64_ptr = (int64_t *)duckdb_vector_get_data(bigint_vector);
	*int64_ptr = 42;

	// append SMALLINT
	auto smallint_vector = duckdb_data_chunk_get_vector(data_chunk, 1);
	auto int16_ptr = (int16_t *)duckdb_vector_get_data(smallint_vector);
	*int16_ptr = 84;

	// append BLOB
	string s = "this is my blob";
	auto blob_vector = duckdb_data_chunk_get_vector(data_chunk, 2);
	duckdb_vector_assign_string_element_len(blob_vector, 0, s.c_str(), s.length());

	REQUIRE(duckdb_vector_get_data(nullptr) == nullptr);

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);

	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
	REQUIRE(duckdb_append_data_chunk(appender, nullptr) == DuckDBError);
	REQUIRE(duckdb_append_data_chunk(nullptr, data_chunk) == DuckDBError);

	// append nulls
	duckdb_data_chunk_reset(data_chunk);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		auto vector = duckdb_data_chunk_get_vector(data_chunk, i);
		duckdb_vector_ensure_validity_writable(vector);
		auto validity = duckdb_vector_get_validity(vector);

		REQUIRE(duckdb_validity_row_is_valid(validity, 0));
		duckdb_validity_set_row_validity(validity, 0, false);
		REQUIRE(!duckdb_validity_row_is_valid(validity, 0));
	}

	duckdb_data_chunk_set_size(data_chunk, 1);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 1);
	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);
	REQUIRE(duckdb_vector_get_validity(nullptr) == nullptr);

	duckdb_appender_destroy(&appender);

	result = tester.Query("SELECT * FROM test");
	REQUIRE_NO_FAIL(*result);

	REQUIRE(result->Fetch<int64_t>(0, 0) == 42);
	REQUIRE(result->Fetch<int16_t>(1, 0) == 84);
	REQUIRE(result->Fetch<string>(2, 0) == "this is my blob");

	REQUIRE(result->IsNull(0, 1));
	REQUIRE(result->IsNull(1, 1));
	REQUIRE(result->IsNull(2, 1));

	duckdb_data_chunk_reset(data_chunk);
	duckdb_data_chunk_reset(nullptr);
	REQUIRE(duckdb_data_chunk_get_size(data_chunk) == 0);

	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(&data_chunk);
	duckdb_destroy_data_chunk(nullptr);

	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}
}

TEST_CASE("Test DataChunk varchar result fetch in C API", "[capi]") {
	if (duckdb_vector_size() < 64) {
		return;
	}

	duckdb_database database;
	duckdb_connection connection;
	duckdb_state state;

	state = duckdb_open(nullptr, &database);
	REQUIRE(state == DuckDBSuccess);
	state = duckdb_connect(database, &connection);
	REQUIRE(state == DuckDBSuccess);

	constexpr const char *VARCHAR_TEST_QUERY = "select case when i != 0 and i % 42 = 0 then NULL else repeat(chr((65 + "
	                                           "(i % 26))::INTEGER), (4 + (i % 12))) end from range(5000) tbl(i);";

	// fetch a small result set
	duckdb_result result;
	state = duckdb_query(connection, VARCHAR_TEST_QUERY, &result);
	REQUIRE(state == DuckDBSuccess);

	REQUIRE(duckdb_column_count(&result) == 1);
	REQUIRE(duckdb_row_count(&result) == 5000);
	REQUIRE(duckdb_result_error(&result) == nullptr);

	idx_t expected_chunk_count = (5000 / STANDARD_VECTOR_SIZE) + (5000 % STANDARD_VECTOR_SIZE != 0);

	REQUIRE(duckdb_result_chunk_count(result) == expected_chunk_count);

	auto chunk = duckdb_result_get_chunk(result, 0);

	REQUIRE(duckdb_data_chunk_get_column_count(chunk) == 1);
	REQUIRE(STANDARD_VECTOR_SIZE < 5000);
	REQUIRE(duckdb_data_chunk_get_size(chunk) == STANDARD_VECTOR_SIZE);
	duckdb_destroy_data_chunk(&chunk);

	idx_t tuple_index = 0;
	auto chunk_amount = duckdb_result_chunk_count(result);
	for (idx_t chunk_index = 0; chunk_index < chunk_amount; chunk_index++) {
		chunk = duckdb_result_get_chunk(result, chunk_index);
		// Our result only has one column
		auto vector = duckdb_data_chunk_get_vector(chunk, 0);
		auto validity = duckdb_vector_get_validity(vector);
		auto string_data = (duckdb_string_t *)duckdb_vector_get_data(vector);

		auto tuples_in_chunk = duckdb_data_chunk_get_size(chunk);
		for (idx_t i = 0; i < tuples_in_chunk; i++, tuple_index++) {
			if (!duckdb_validity_row_is_valid(validity, i)) {
				// This entry is NULL
				REQUIRE((tuple_index != 0 && tuple_index % 42 == 0));
				continue;
			}
			idx_t expected_length = (tuple_index % 12) + 4;
			char expected_character = (tuple_index % 26) + 'A';

			// TODO: how does the c-api handle non-flat vectors?
			auto tuple = string_data[i];
			auto length = tuple.value.inlined.length;
			REQUIRE(length == expected_length);
			if (duckdb_string_is_inlined(tuple)) {
				// The data is small enough to fit in the string_t, it does not have a separate allocation
				for (idx_t string_index = 0; string_index < length; string_index++) {
					REQUIRE(tuple.value.inlined.inlined[string_index] == expected_character);
				}
			} else {
				for (idx_t string_index = 0; string_index < length; string_index++) {
					REQUIRE(tuple.value.pointer.ptr[string_index] == expected_character);
				}
			}
		}
		duckdb_destroy_data_chunk(&chunk);
	}
	duckdb_destroy_result(&result);
	duckdb_disconnect(&connection);
	duckdb_close(&database);
}

TEST_CASE("Test DataChunk result fetch in C API", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	if (duckdb_vector_size() < 64) {
		return;
	}

	REQUIRE(tester.OpenDatabase(nullptr));

	// fetch a small result set
	result = tester.Query("SELECT CASE WHEN i=1 THEN NULL ELSE i::INTEGER END i FROM range(3) tbl(i)");
	REQUIRE(NO_FAIL(*result));
	REQUIRE(result->ColumnCount() == 1);
	REQUIRE(result->row_count() == 3);
	REQUIRE(result->ErrorMessage() == nullptr);

	// fetch the first chunk
	REQUIRE(result->ChunkCount() == 1);
	auto chunk = result->FetchChunk(0);
	REQUIRE(chunk);

	REQUIRE(chunk->ColumnCount() == 1);
	REQUIRE(chunk->size() == 3);

	auto data = (int32_t *)chunk->GetData(0);
	auto validity = chunk->GetValidity(0);
	REQUIRE(data[0] == 0);
	REQUIRE(data[2] == 2);
	REQUIRE(duckdb_validity_row_is_valid(validity, 0));
	REQUIRE(!duckdb_validity_row_is_valid(validity, 1));
	REQUIRE(duckdb_validity_row_is_valid(validity, 2));

	// after fetching a chunk, we cannot use the old API anymore
	REQUIRE(result->ColumnData<int32_t>(0) == nullptr);
	REQUIRE(result->Fetch<int32_t>(0, 1) == 0);

	// result set is exhausted!
	chunk = result->FetchChunk(1);
	REQUIRE(!chunk);
}

TEST_CASE("Test duckdb_result_return_type", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));

	result = tester.Query("CREATE TABLE t (id INT)");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_NOTHING);

	result = tester.Query("INSERT INTO t VALUES (42)");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_CHANGED_ROWS);

	result = tester.Query("FROM t");
	REQUIRE(duckdb_result_return_type(result->InternalResult()) == DUCKDB_RESULT_TYPE_QUERY_RESULT);
}

TEST_CASE("Test DataChunk populate ListVector in C API", "[capi]") {
	if (duckdb_vector_size() < 3) {
		return;
	}
	REQUIRE(duckdb_list_vector_reserve(nullptr, 100) == duckdb_state::DuckDBError);
	REQUIRE(duckdb_list_vector_set_size(nullptr, 200) == duckdb_state::DuckDBError);

	auto elem_type = duckdb_create_logical_type(duckdb_type::DUCKDB_TYPE_INTEGER);
	auto list_type = duckdb_create_list_type(elem_type);
	duckdb_logical_type schema[] = {list_type};
	auto chunk = duckdb_create_data_chunk(schema, 1);
	auto list_vector = duckdb_data_chunk_get_vector(chunk, 0);
	duckdb_data_chunk_set_size(chunk, 3);

	REQUIRE(duckdb_list_vector_reserve(list_vector, 123) == duckdb_state::DuckDBSuccess);
	REQUIRE(duckdb_list_vector_get_size(list_vector) == 0);
	auto child = duckdb_list_vector_get_child(list_vector);
	for (int i = 0; i < 123; i++) {
		((int *)duckdb_vector_get_data(child))[i] = i;
	}
	REQUIRE(duckdb_list_vector_set_size(list_vector, 123) == duckdb_state::DuckDBSuccess);
	REQUIRE(duckdb_list_vector_get_size(list_vector) == 123);

	auto entries = (duckdb_list_entry *)duckdb_vector_get_data(list_vector);
	entries[0].offset = 0;
	entries[0].length = 20;
	entries[1].offset = 20;
	entries[1].length = 80;
	entries[2].offset = 100;
	entries[2].length = 23;

	auto child_data = (int *)duckdb_vector_get_data(child);
	int count = 0;
	for (idx_t i = 0; i < duckdb_data_chunk_get_size(chunk); i++) {
		for (idx_t j = 0; j < entries[i].length; j++) {
			REQUIRE(child_data[entries[i].offset + j] == count);
			count++;
		}
	}
	auto &vector = (Vector &)(*list_vector);
	for (int i = 0; i < 123; i++) {
		REQUIRE(ListVector::GetEntry(vector).GetValue(i) == i);
	}

	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&list_type);
	duckdb_destroy_logical_type(&elem_type);
}

TEST_CASE("Test DataChunk populate ArrayVector in C API", "[capi]") {

	auto elem_type = duckdb_create_logical_type(duckdb_type::DUCKDB_TYPE_INTEGER);
	auto array_type = duckdb_create_array_type(elem_type, 3);
	duckdb_logical_type schema[] = {array_type};
	auto chunk = duckdb_create_data_chunk(schema, 1);
	duckdb_data_chunk_set_size(chunk, 2);
	auto array_vector = duckdb_data_chunk_get_vector(chunk, 0);

	auto child = duckdb_array_vector_get_child(array_vector);
	for (int i = 0; i < 6; i++) {
		((int *)duckdb_vector_get_data(child))[i] = i;
	}

	auto vec = (Vector &)(*array_vector);
	for (int i = 0; i < 2; i++) {
		auto child_vals = ArrayValue::GetChildren(vec.GetValue(i));
		for (int j = 0; j < 3; j++) {
			REQUIRE(child_vals[j].GetValue<int>() == i * 3 + j);
		}
	}

	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&array_type);
	duckdb_destroy_logical_type(&elem_type);
}

TEST_CASE("Test PK violation in the C API appender", "[capi]") {
	CAPITester tester;
	duckdb::unique_ptr<CAPIResult> result;

	REQUIRE(tester.OpenDatabase(nullptr));
	REQUIRE(duckdb_vector_size() == STANDARD_VECTOR_SIZE);

	// Create column types.
	const idx_t COLUMN_COUNT = 1;
	duckdb_logical_type types[COLUMN_COUNT];
	types[0] = duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);

	// Create data chunk.
	auto data_chunk = duckdb_create_data_chunk(types, COLUMN_COUNT);
	auto bigint_vector = duckdb_data_chunk_get_vector(data_chunk, 0);
	auto int64_ptr = reinterpret_cast<int64_t *>(duckdb_vector_get_data(bigint_vector));
	int64_ptr[0] = 42;
	int64_ptr[1] = 42;
	duckdb_data_chunk_set_size(data_chunk, 2);

	// Use the appender to append the data chunk.
	tester.Query("CREATE TABLE test(i BIGINT PRIMARY KEY)");
	duckdb_appender appender;
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", &appender) == DuckDBSuccess);

	// We only flush when destroying the appender. Thus, we expect this to succeed, as we only
	// detect constraint violations when flushing the results.
	REQUIRE(duckdb_append_data_chunk(appender, data_chunk) == DuckDBSuccess);

	// duckdb_appender_close attempts to flush the data and fails.
	auto state = duckdb_appender_close(appender);
	REQUIRE(state == DuckDBError);
	auto error = duckdb_appender_error(appender);
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violation"));

	// Destroy the appender despite the error to avoid leaks.
	state = duckdb_appender_destroy(&appender);
	REQUIRE(state == DuckDBError);

	// Clean-up.
	duckdb_destroy_data_chunk(&data_chunk);
	for (idx_t i = 0; i < COLUMN_COUNT; i++) {
		duckdb_destroy_logical_type(&types[i]);
	}

	// Ensure that no rows were appended.
	result = tester.Query("SELECT * FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 0);

	// Try again by appending rows and flushing.
	REQUIRE(duckdb_appender_create(tester.connection, nullptr, "test", &appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int64(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_appender_begin_row(appender) == DuckDBSuccess);
	REQUIRE(duckdb_append_int64(appender, 42) == DuckDBSuccess);
	REQUIRE(duckdb_appender_end_row(appender) == DuckDBSuccess);

	state = duckdb_appender_flush(appender);
	REQUIRE(state == DuckDBError);
	error = duckdb_appender_error(appender);
	REQUIRE(duckdb::StringUtil::Contains(error, "PRIMARY KEY or UNIQUE constraint violation"));
	REQUIRE(duckdb_appender_destroy(&appender) == DuckDBError);

	// Ensure that only the last row was appended.
	result = tester.Query("SELECT * FROM test;");
	REQUIRE_NO_FAIL(*result);
	REQUIRE(result->row_count() == 0);
}

TEST_CASE("Test DataChunk write BLOB", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_BLOB);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_BLOB);
	duckdb_logical_type types[] = {type};
	auto chunk = duckdb_create_data_chunk(types, 1);
	duckdb_data_chunk_set_size(chunk, 1);
	duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, 0);
	auto column_type = duckdb_vector_get_column_type(vector);
	REQUIRE(duckdb_get_type_id(column_type) == DUCKDB_TYPE_BLOB);
	duckdb_destroy_logical_type(&column_type);
	uint8_t bytes[] = {0x80, 0x00, 0x01, 0x2a};
	duckdb_vector_assign_string_element_len(vector, 0, (const char *)bytes, 4);
	auto string_data = static_cast<duckdb_string_t *>(duckdb_vector_get_data(vector));
	auto string_value = duckdb_string_t_data(string_data);
	REQUIRE(duckdb_string_t_length(*string_data) == 4);
	REQUIRE(string_value[0] == (char)0x80);
	REQUIRE(string_value[1] == (char)0x00);
	REQUIRE(string_value[2] == (char)0x01);
	REQUIRE(string_value[3] == (char)0x2a);
	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&type);
}

TEST_CASE("Test DataChunk write VARINT", "[capi]") {
	duckdb_logical_type type = duckdb_create_logical_type(DUCKDB_TYPE_VARINT);
	REQUIRE(type);
	REQUIRE(duckdb_get_type_id(type) == DUCKDB_TYPE_VARINT);
	duckdb_logical_type types[] = {type};
	auto chunk = duckdb_create_data_chunk(types, 1);
	duckdb_data_chunk_set_size(chunk, 1);
	duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, 0);
	auto column_type = duckdb_vector_get_column_type(vector);
	REQUIRE(duckdb_get_type_id(column_type) == DUCKDB_TYPE_VARINT);
	duckdb_destroy_logical_type(&column_type);
	uint8_t bytes[] = {0x80, 0x00, 0x01, 0x2a}; // VARINT 42
	duckdb_vector_assign_string_element_len(vector, 0, (const char *)bytes, 4);
	auto string_data = static_cast<duckdb_string_t *>(duckdb_vector_get_data(vector));
	auto string_value = duckdb_string_t_data(string_data);
	REQUIRE(duckdb_string_t_length(*string_data) == 4);
	REQUIRE(string_value[0] == (char)0x80);
	REQUIRE(string_value[1] == (char)0x00);
	REQUIRE(string_value[2] == (char)0x01);
	REQUIRE(string_value[3] == (char)0x2a);
	duckdb_destroy_data_chunk(&chunk);
	duckdb_destroy_logical_type(&type);
}
