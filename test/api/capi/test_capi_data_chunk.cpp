#include "capi_tester.hpp"

#include <duckdb/main/capi/capi_internal.hpp>

namespace duckdb {
struct DuckDBResultData;
}
using namespace duckdb;
using namespace std;

TEST_CASE("Test table_info incorrect 'is_valid' value for 'dflt_value' column", "[capi]") {
	duckdb_database db;
	duckdb_connection con;
	duckdb_result result;

	// if(duckdb_query(con, "ATTACH '../resources/tpch-sf1.db' AS file_db;", &result) != DuckDBSuccess) {
	// 	fprintf(stderr, "Failed to query attach file db.\n");
	// }

	REQUIRE(duckdb_open(NULL, &db) != DuckDBError);
	REQUIRE(duckdb_connect(db, &con) != DuckDBError);

	REQUIRE(duckdb_query(con, "ATTACH 'transfer.db' AS file_db;", &result) == DuckDBSuccess);

	REQUIRE(duckdb_query(con, "ATTACH ':memory:' AS mem_db;", &result) == DuckDBSuccess);

	REQUIRE(duckdb_query(con, "COPY FROM DATABASE file_db TO mem_db;", &result) == DuckDBSuccess);
	REQUIRE(duckdb_query(con, "USE mem_db;", &result) == DuckDBSuccess);

	REQUIRE(duckdb_query(con, "SELECT * FROM cp;", &result) == DuckDBSuccess);

	// auto &result_data = *(reinterpret_cast<duckdb::DuckDBResultData *>(result.internal_data));
	// result_data.result_set_type = duckdb::CAPIResultSetType::CAPI_RESULT_TYPE_MATERIALIZED;
	// auto &materialized = reinterpret_cast<duckdb::MaterializedQueryResult &>(*result_data.result);
	//
	// // get a value first.
	// idx_t chunk_count = duckdb_result_chunk_count(result);
	// auto & collection = materialized.Collection();
	//
	// std::vector<void*> values;
	// values.reserve(collection.ColumnCount() * chunk_count);
	//
	// for (auto &chunk : collection.Chunks()) {
	// 	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
	// 		values.push_back(chunk.data[c].GetData());
	// 	}
	//
	// 	// for string I need to unmarshall it anyways.. so I just need to know the length.
	// 	// just need to know which string block it is.
	// 	// the problem here is that, I need to know which one is zipped and which one is not..
	// 	// at least I need to find the first one that's zipped.
	// 	// but need to know if those are contigious.
	// 	auto x = (string_t*)chunk.data[13].GetData();
	//
	// }

	chunk_results ptrs = duckdb_chunk_data_ptrs(result);



	// while(true) {
	duckdb_data_chunk chunk = duckdb_result_get_chunk(result, 0);
	duckdb_data_chunk chunk1 = duckdb_result_get_chunk(result, 1);

		// if(!chunk) {
		// 	break;
		// }

	uint64_t* order_key = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 0));
	uint64_t* order_key1 = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk1, 0));
	uint64_t* part_key = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 1));
	uint64_t* part_key1 = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk1, 1));
	uint64_t* supp_key = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 2));
	uint64_t* line_number = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 3));

	uint64_t* quantity = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 4));
	uint64_t* extended_price = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 5));
	uint64_t* discount= (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 6));
	uint64_t* tax = (uint64_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 7));

	duckdb_string_t* return_flag = (duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 8));
	duckdb_string_t* line_status = (duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 9));

	int32_t* ship_date = (int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 10));
	int32_t* commit_date = (int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 11));
	int32_t* receipt_date = (int32_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 12));

	duckdb_string_t* ship_instruct = (duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 13));
	duckdb_string_t* ship_mode = (duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 14));
	duckdb_string_t* comment = (duckdb_string_t*)duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 15));
		duckdb_destroy_data_chunk(&chunk);
		duckdb_destroy_data_chunk(&chunk1);
	// }

	duckdb_destroy_result(&result);
	duckdb_disconnect(&con);
	duckdb_close(&db);
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
