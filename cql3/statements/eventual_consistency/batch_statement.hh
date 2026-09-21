/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_statement.hh"
#include "exceptions/coordinator_result.hh"
#include "service_permit.hh"
#include "tracing/trace_state.hh"
#include "utils/log.hh"

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3::statements::eventual_consistency {

/*
 * A batch committed through storage_proxy: its modifications become one
 * mutation per partition, written with the replication factor's eventual
 * consistency, or a single Paxos round when any of them carries IF conditions.
 */
class batch_statement final : public cql3::statements::batch_statement {
    static logging::logger _logger;

public:
    batch_statement(int bound_terms, type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    batch_statement(type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    /**
     * Checks batch size to ensure threshold is met. If not, a warning is logged.
     * @param cfs ColumnFamilies that will store the batch's mutations.
     */
    void verify_batch_size(query_processor& qp, const utils::chunked_vector<mutation>& mutations) const;

    future<shared_ptr<cql_transport::messages::result_message>> execute(
            query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<shared_ptr<cql_transport::messages::result_message>> execute_without_checking_exception_message(
            query_processor& qp, service::query_state& state, const query_options& options, std::optional<service::group0_guard> guard) const override;

private:
    future<utils::chunked_vector<mutation>> get_mutations(query_processor& qp, const query_options& options, db::timeout_clock::time_point timeout,
            bool local, api::timestamp_type now, service::query_state& query_state) const;

    future<shared_ptr<cql_transport::messages::result_message>> do_execute(
            query_processor& qp,
            service::query_state& query_state, const query_options& options,
            bool local, api::timestamp_type now) const;

    future<exceptions::coordinator_result<>> execute_without_conditions(
            query_processor& qp,
            utils::chunked_vector<mutation> mutations,
            db::consistency_level cl,
            db::timeout_clock::time_point timeout,
            tracing::trace_state_ptr tr_state,
            service_permit permit,
            db::large_data_violation_type* violations) const;

    future<shared_ptr<cql_transport::messages::result_message>> execute_with_conditions(
            query_processor& qp,
            const query_options& options,
            service::query_state& state) const;

    friend class batch_statement_executor;
};

}
