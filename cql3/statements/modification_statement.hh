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

#include <memory>
#include <optional>

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3 {

class query_processor;

namespace statements {

namespace raw { class modification_statement; }

/*
 * Abstract parent class of individual modifications, i.e. INSERT, UPDATE and DELETE,
 * as the CQL server executes them: through storage_proxy, or through Paxos when the
 * modification carries IF conditions.
 *
 * Inheriting the modification rather than holding it is scaffolding: it keeps the
 * commits that move parse state into modification_spec pure moves. A later commit in
 * this series turns the base into a member.
 */
class modification_statement : public cql_statement, public modification_spec {
public:
    modification_statement(
            statement_type type_,
            uint32_t bound_terms,
            schema_ptr schema_,
            std::unique_ptr<attributes> attrs_,
            cql_stats& stats_);

    ~modification_statement();

    // Both bases declare it; the statement's own is the one callers mean.
    using cql_statement::get_timeout_config_selector;

    // The modification this statement executes.
    const modification_spec& spec() const { return *this; }

    uint32_t get_bound_terms() const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    bool should_reclassify_control_connection() const override;

    void validate(query_processor& qp, const service::client_state& state) const override;

    seastar::shared_ptr<const metadata> get_result_metadata() const override;

    bool is_conditional() const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const;
    friend class modification_statement_executor;

    future<exceptions::coordinator_result<>>
    execute_without_condition(query_processor& qp, service::query_state& qs, const query_options& options, json_cache_opt& json_cache, std::vector<dht::partition_range> keys, db::large_data_violation_type* violations) const;

    future<::shared_ptr<cql_transport::messages::result_message>>
    execute_with_condition(query_processor& qp, service::query_state& qs, const query_options& options) const;

    friend class raw::modification_statement;
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
