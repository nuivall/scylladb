/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/modification_statement.hh"

namespace cql3::statements::strong_consistency {

/*
 * A modification committed through the Raft group which owns the partition it
 * addresses, rather than through storage_proxy.
 */
class modification_statement final : public cql3::statements::modification_statement {
    using result_message = cql_transport::messages::result_message;

public:
    using cql3::statements::modification_statement::modification_statement;

    future<shared_ptr<result_message>> execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<shared_ptr<result_message>> execute_without_checking_exception_message(query_processor& qp,
        service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const override;
};

// Builds the single mutation a strongly consistent modification produces for the
// given partition key and timestamp. Shared by single modifications and batches,
// which build one mutation per modification and merge them.
mutation build_mutation(const modification_spec& spec, const query_options& options, api::timestamp_type ts,
        const modification_spec::json_cache_opt& json_cache, const std::vector<dht::partition_range>& keys);

}
