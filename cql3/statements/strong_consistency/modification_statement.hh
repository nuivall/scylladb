/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_spec.hh"

namespace cql3::statements::strong_consistency {

/*
 * A modification committed through the Raft group which owns the partition it
 * addresses, rather than through storage_proxy.
 */
class modification_statement : public cql_statement {
    using result_message = cql_transport::messages::result_message;

    ::shared_ptr<modification_spec> _spec;
public:
    explicit modification_statement(::shared_ptr<modification_spec> spec);

    // What this statement commits. Borrowed, so only valid while it lives.
    const modification_spec& spec() const { return *_spec; }

    // The same, for a caller which has to keep it alive on its own.
    const ::shared_ptr<modification_spec>& shared_spec() const { return _spec; }

    future<shared_ptr<result_message>> execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<shared_ptr<result_message>> execute_without_checking_exception_message(query_processor& qp,
        service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    void validate(query_processor& qp, const service::client_state& state) const override;

    uint32_t get_bound_terms() const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    // Carries user load exactly when the modification it commits does.
    bool should_reclassify_control_connection() const override {
        return _spec->should_reclassify_control_connection();
    }
};

// Builds the single mutation a strongly consistent modification produces for the
// given partition key and timestamp. Shared by single modifications and batches,
// which build one mutation per modification and merge them.
mutation build_mutation(const modification_spec& spec, const query_options& options, api::timestamp_type ts,
        const modification_spec::json_cache_opt& json_cache, const std::vector<dht::partition_range>& keys);

}
