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
    bool _may_use_token_aware_routing;

    typedef std::optional<std::unordered_map<sstring, bytes_opt>> json_cache_opt;

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

    // Build a read_command instance to fetch the previous mutation from storage. The mutation is
    // fetched if we need to check LWT conditions or apply updates to non-frozen list elements.
    lw_shared_ptr<query::read_command> read_command(query_processor& qp, query::clustering_row_ranges ranges, db::consistency_level cl) const;

    /// Checks that the primary key the statement names has no null values, throwing
    /// invalid_request_exception otherwise.
    virtual void validate_primary_key(const query_options& options) const = 0;

    virtual dht::partition_range_vector build_partition_keys(const query_options& options, const json_cache_opt& json_cache) const = 0;
    virtual query::clustering_row_ranges create_clustering_ranges(const query_options& options, const json_cache_opt& json_cache) const = 0;

    // Create a mutation object for the update operation represented by this modification statement.
    // A single mutation object for lightweight transactions, which can only span one partition, or a vector
    // of mutations, one per partition key, for statements which affect multiple partition keys,
    // e.g. DELETE FROM table WHERE pk  IN (1, 2, 3).
    virtual utils::chunked_vector<mutation> apply_updates(
            const std::vector<dht::partition_range>& keys,
            const std::vector<query::clustering_range>& ranges,
            const update_parameters& params,
            const json_cache_opt& json_cache) const = 0;

protected:
    // One empty mutation per partition the statement addresses, for apply_updates()
    // to write rows into.
    utils::chunked_vector<mutation> make_mutations(const std::vector<dht::partition_range>& keys) const;

public:
    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const override;

    /**
     * Convert statement into a list of mutations to apply on the server
     *
     * @param options value for prepared statement markers
     * @param local if true, any requests (for collections) performed by getMutation should be done locally only.
     * @param now the current timestamp in microseconds to use if no timestamp is user provided.
     *
     * @return vector of the mutations
     * @throws invalid_request_exception on invalid requests
     */
    future<utils::chunked_vector<mutation>> get_mutations(query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout, bool local, int64_t now, service::query_state& qs, json_cache_opt& json_cache, std::vector<dht::partition_range> keys) const;

    virtual json_cache_opt maybe_prepare_json_cache(const query_options& options) const;

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

}

}
