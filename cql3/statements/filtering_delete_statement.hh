/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/* Copyright 2026-present ScyllaDB */

#pragma once

#include "cql3/statements/delete_statement.hh"
#include "query/query-request.hh"

namespace cql3 {

namespace statements {

// A DELETE statement with ALLOW FILTERING. This statement scans data across
// the cluster in parallel (mapreduce-style) and applies tombstones for
// matching rows/partitions. Unlike a normal DELETE, it does not require
// the full partition key to be specified.
class filtering_delete_statement : public delete_statement {
public:
    filtering_delete_statement(audit::audit_info_ptr&& audit_info, statement_type type, uint32_t bound_terms,
                               schema_ptr s, std::unique_ptr<attributes> attrs, cql_stats& stats);

    virtual future<::shared_ptr<cql_transport::messages::result_message>>
    execute_without_checking_exception_message(query_processor& qp, service::query_state& qs,
            const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    future<::shared_ptr<cql_transport::messages::result_message>>
    do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const;

    // Determine optimization tier based on the restrictions
    query::filtering_delete_request::tier classify() const;
};

}

}
