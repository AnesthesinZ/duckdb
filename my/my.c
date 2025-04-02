#include <stdio.h>
#include <unistd.h>

#include "../src/include/duckdb.h"

int main(int argc, char *argv[])
{
    // this is the testing playground

    duckdb_database db;
    duckdb_connection db_conn;

    // Open disk-based DuckDB
    if (duckdb_open("", &db) != DuckDBSuccess) {
        fprintf(stderr, "Failed to open disk database\n");
        return 1;
    }

    // Create connections
    if (duckdb_connect(db, &db_conn) != DuckDBSuccess) {
        fprintf(stderr, "Error: Could not connect to database.\n");
        duckdb_close(&db);
        return 1;
    }

    duckdb_result result;
    // load whole db into memory
    if(duckdb_query(db_conn, "ATTACH '../resources/tpch-sf1.db' AS file_db;", &result) != DuckDBSuccess) {
        fprintf(stderr, "Failed to query attach file db.\n");
    }

    if(duckdb_query(db_conn, "ATTACH ':memory:' AS mem_db;", &result) != DuckDBSuccess) {
        fprintf(stderr, "Failed to query attach mem db.\n");
    }

    if(duckdb_query(db_conn, "COPY FROM DATABASE file_db TO mem_db;", &result) != DuckDBSuccess) {
        fprintf(stderr, "Failed to copy to mem_db.\n");
    }

    if (duckdb_query(db_conn, "SELECT * FROM mem_db.lineitem;", &result) != DuckDBSuccess) {
        fprintf(stderr, "Query execution failed\n");
        return 1;
    }

    struct timeval query_start_time, query_end_time;

    while(true) {
        duckdb_data_chunk chunk = duckdb_fetch_chunk(result);

        if(!chunk) {
            break;
        }

        uint64_t* order_key = duckdb_vector_get_data(duckdb_data_chunk_get_vector(chunk, 0));

        duckdb_destroy_data_chunk(&chunk);
    }

    duckdb_destroy_result(&result);
    duckdb_disconnect(&db_conn);
    duckdb_close(&db);
}
