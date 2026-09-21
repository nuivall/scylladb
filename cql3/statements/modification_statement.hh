/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_spec.hh"
#include "exceptions/coordinator_result.hh"

#include <seastar/core/shared_ptr.hh>

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3 {

namespace statements {

/*
 * A single modification - an INSERT, an UPDATE or a DELETE - as the CQL server
 * executes it.
 *
 * The statement holds the modification_spec that parsing produced and adds
 * nothing to it but execution: committing the spec's mutation through
 * storage_proxy, or through Paxos when it carries IF conditions. Everything
 * else a cql_statement is asked - access control, validation, the bound terms -
 * the spec answers.
 */
class modification_statement : public cql_statement {
    const ::shared_ptr<modification_spec> _spec;

public:
    explicit modification_statement(::shared_ptr<modification_spec> spec);

    virtual ~modification_statement() override;

    // What this statement executes. Borrowed, so only valid while it lives.
    const modification_spec& spec() const { return *_spec; }

    // The same, for a caller which has to keep the spec alive on its own, e.g.
    // a batch collecting the modifications it commits together.
    const ::shared_ptr<modification_spec>& shared_spec() const { return _spec; }

    uint32_t get_bound_terms() const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    void validate(query_processor& qp, const service::client_state& state) const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    bool should_reclassify_control_connection() const override;

    bool is_conditional() const override;

    seastar::shared_ptr<const metadata> get_result_metadata() const override;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const;

    future<exceptions::coordinator_result<>>
    execute_without_condition(query_processor& qp, service::query_state& qs, const query_options& options,
            modification_spec::json_cache_opt& json_cache, std::vector<dht::partition_range> keys,
            db::large_data_violation_type* violations) const;

    future<::shared_ptr<cql_transport::messages::result_message>>
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

}
