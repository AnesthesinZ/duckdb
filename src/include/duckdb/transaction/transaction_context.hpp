//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/transaction_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/assert.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {

class ClientContext;
class MetaTransaction;
class Transaction;
class TransactionManager;

//! The transaction context keeps track of all the information relating to the
//! current transaction
class TransactionContext {
public:
	explicit TransactionContext(ClientContext &context);
	~TransactionContext();

	MetaTransaction &ActiveTransaction() {
		if (!current_transaction) {
			throw InternalException("TransactionContext::ActiveTransaction called without active transaction");
		}
		return *current_transaction;
	}

	bool HasActiveTransaction() const {
		return current_transaction.get();
	}

	void BeginTransaction();
	void Commit();
	void Rollback(optional_ptr<ErrorData>);
	void ClearTransaction();

	void SetAutoCommit(bool value);
	bool IsAutoCommit() const {
		return auto_commit;
	}

	void SetReadOnly();

	idx_t GetActiveQuery();
	void ResetActiveQuery();
	void SetActiveQuery(transaction_t query_number);

	void SetDataPointer(std::vector<duckdb::SegmentPlaceHolder>* data_ptr_pointer) {
		if(HasActiveTransaction()) {
			current_transaction->SetDataPointer(data_ptr_pointer);
		}
	}

	std::vector<duckdb::SegmentPlaceHolder>* GetDataPointer() {
		if(HasActiveTransaction()) {
			return current_transaction->GetDataPointer();
		}
		return nullptr;
	}

	bool isAppendPlaceHolder() const {
		if(HasActiveTransaction()) {
			return current_transaction->isAppendPlaceHolder();
		}
		return false;
	}

private:
	ClientContext &context;
	bool auto_commit;

	unique_ptr<MetaTransaction> current_transaction;

	TransactionContext(const TransactionContext &) = delete;
};

} // namespace duckdb
