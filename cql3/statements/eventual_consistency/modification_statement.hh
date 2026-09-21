/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/modification_statement.hh"
#include "exceptions/coordinator_result.hh"

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3::statements::eventual_consistency {

/*
 * A modification committed through storage_proxy, with the replication factor's
 * eventual consistency: a plain write, or a Paxos round when the modification
 * carries IF conditions.
 */
class modification_statement final : public cql3::statements::modification_statement {
    using result_message = cql_transport::messages::result_message;

public:
    using cql3::statements::modification_statement::modification_statement;

    future<::shared_ptr<result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<::shared_ptr<result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    future<::shared_ptr<result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const;

    future<exceptions::coordinator_result<>>
    execute_without_condition(query_processor& qp, service::query_state& qs, const query_options& options,
            modification_spec::json_cache_opt& json_cache, std::vector<dht::partition_range> keys,
            db::large_data_violation_type* violations) const;

    future<::shared_ptr<result_message>>
    execute_with_condition(query_processor& qp, service::query_state& qs, const query_options& options) const;

    friend class modification_statement_executor;
};

/**
 * Converts a modification into the mutations to apply on the server, reading the
 * old row through storage_proxy first when the modification needs one.
 *
 * Free, because a batch commits the modifications it holds without ever
 * executing their statements.
 *
 * @param options value for prepared statement markers
 * @param local if true, any requests (for collections) performed should be done locally only.
 * @param now the current timestamp in microseconds to use if no timestamp is user provided.
 */
future<utils::chunked_vector<mutation>> get_mutations(const modification_spec& spec, query_processor& qp,
        const query_options& options, db::timeout_clock::time_point timeout, bool local, int64_t now,
        service::query_state& qs, modification_spec::json_cache_opt& json_cache,
        std::vector<dht::partition_range> keys);

}
