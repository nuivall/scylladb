/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

/* Copyright 2026-present ScyllaDB */

#include "cql3/statements/filtering_delete_statement.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/query_processor.hh"
#include "cql3/util.hh"
#include "db/consistency_level_validations.hh"
#include "gms/feature_service.hh"
#include "service/storage_proxy.hh"
#include "transport/messages/result_message.hh"
#include "tracing/tracing.hh"
#include "validation.hh"

namespace cql3 {

namespace statements {

filtering_delete_statement::filtering_delete_statement(
    audit::audit_info_ptr&& audit_info,
    statement_type type,
    uint32_t bound_terms,
    schema_ptr s,
    std::unique_ptr<attributes> attrs,
    cql_stats& stats)
    : delete_statement(std::move(audit_info), type, bound_terms, std::move(s), std::move(attrs), stats)
{
}

query::filtering_delete_request::tier
filtering_delete_statement::classify() const {
    using tier = query::filtering_delete_request::tier;
    auto& r = restrictions();
    if (r.has_non_primary_key_restriction()) {
        bool all_static = std::ranges::all_of(
                r.get_non_pk_restriction(),
                [] (const auto& entry) { return entry.first->is_static(); });
        if (all_static && !r.has_clustering_columns_restriction()) {
            return tier::partition_only;
        }
        return tier::regular_column;
    }
    if (r.has_clustering_columns_restriction()) {
        return tier::clustering;
    }
    return tier::partition_only;
}

future<::shared_ptr<cql_transport::messages::result_message>>
filtering_delete_statement::execute_without_checking_exception_message(
    query_processor& qp,
    service::query_state& qs,
    const query_options& options,
    std::optional<service::group0_guard> guard) const
{
    if (!qp.proxy().features().filtering_delete) {
        throw exceptions::invalid_request_exception(
            "DELETE with ALLOW FILTERING is not yet supported by all nodes in the cluster. "
            "Please ensure all nodes are upgraded before using this feature.");
    }
    return do_execute(qp, qs, options);
}

future<::shared_ptr<cql_transport::messages::result_message>>
filtering_delete_statement::do_execute(
    query_processor& qp,
    service::query_state& qs,
    const query_options& options) const
{
    cql3::util::validate_timestamp(qp.db().get_config(), options, attrs);
    (void)validation::validate_column_family(qp.db(), keyspace(), column_family());

    tracing::add_table_name(qs.get_trace_state(), keyspace(), column_family());

    inc_cql_stats(qs.get_client_state().is_internal());
    ++_stats.filtered_deletes;

    auto cl = options.get_consistency();
    if (db::is_serial_consistency(cl)) {
        throw exceptions::invalid_request_exception(
            "SERIAL/LOCAL_SERIAL consistency is not supported for DELETE with ALLOW FILTERING");
    }

    // Inline bind variables into the WHERE expression and serialize to CQL text.
    // This makes the expression self-contained for transmission to remote nodes.
    auto& where_expr = restrictions().where();
    if (!where_expr) {
        throw exceptions::invalid_request_exception(
            "DELETE with ALLOW FILTERING requires a WHERE clause");
    }
    auto inlined = expr::inline_bind_variables(*where_expr, options);
    auto where_clause = cql3::util::relations_to_where_clause(inlined);

    // Compute partition key ranges from restrictions on the coordinator
    auto key_ranges = restrictions().get_partition_key_ranges(options);

    auto timeout_duration = get_timeout(qs.get_client_state(), options);
    auto deadline = lowres_system_clock::now() + std::chrono::duration_cast<lowres_system_clock::duration>(timeout_duration);

    auto tier = classify();

    query::filtering_delete_request req{
        .schema_id = s->id(),
        .schema_version = s->version(),
        .where_clause = std::move(where_clause),
        .pr = std::move(key_ranges),
        .cl = cl,
        .deadline = deadline,
        .timestamp = options.get_timestamp(qs),
        .optimization_tier = tier,
        .shard_id_hint = std::nullopt,
    };

    tracing::trace(qs.get_trace_state(), "Dispatching filtering delete ({}) to mapreduce service",
                    tier == query::filtering_delete_request::tier::partition_only ? "partition_only" :
                    (tier == query::filtering_delete_request::tier::clustering ? "clustering" : "regular_column"));

    auto result = co_await qp.filtering_delete(std::move(req), qs.get_trace_state());

    tracing::trace(qs.get_trace_state(), "Filtering delete completed: {} rows deleted", result.rows_deleted);

    co_return ::make_shared<cql_transport::messages::result_message::void_message>();
}

} // namespace statements

} // namespace cql3
