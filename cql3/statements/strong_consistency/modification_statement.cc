/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "modification_statement.hh"

#include "db/consistency_level_type.hh"
#include "db/timeout_clock.hh"
#include "service/strong_consistency/groups_manager.hh"
#include "transport/messages/result_message.hh"
#include "cql3/query_processor.hh"
#include "service/strong_consistency/coordinator.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "exceptions/exceptions.hh"
#include "utils/error_injection.hh"

namespace cql3::statements::strong_consistency {

static logging::logger logger("sc_modification_statement");

using result_message = cql_transport::messages::result_message;

mutation build_mutation(const modification_spec& spec, const query_options& options, api::timestamp_type ts,
        const modification_spec::json_cache_opt& json_cache, const std::vector<dht::partition_range>& keys) {
    const auto ranges = spec.create_clustering_ranges(options, json_cache);
    // Nothing is prefetched: a modification that would have to read the old row
    // is rejected when a strongly consistent keyspace prepares it.
    auto muts = spec.build_mutations(options, ts, keys, ranges, json_cache, update_parameters::prefetch_data(spec.s));
    if (muts.size() != 1) {
        on_internal_error(logger, ::format("modification of {}.{} has unexpected number of mutations {}",
            spec.keyspace(), spec.column_family(), muts.size()));
    }
    return std::move(*muts.begin());
}

future<shared_ptr<result_message>> modification_statement::execute(query_processor& qp, service::query_state& qs, 
    const query_options& options, std::optional<service::group0_guard> guard) const
{
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

future<shared_ptr<result_message>> modification_statement::execute_without_checking_exception_message(
        query_processor& qp, service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const
{
    const modification_spec& spec = this->spec();

    validate_write_consistency_level(options.get_consistency());
    spec.validate_primary_key(options);

    auto timeout = db::timeout_clock::now() + spec.get_timeout(qs.get_client_state(), options);
    auto json_cache = spec.maybe_prepare_json_cache(options);
    const auto keys = spec.build_partition_keys(options, json_cache);
    if (keys.size() != 1 || !query::is_single_partition(keys[0])) {
        throw exceptions::invalid_request_exception("Strongly consistent queries can only target a single partition");
    }

    auto [coordinator, holder] = qp.acquire_strongly_consistent_coordinator();
    const auto token = keys[0].start()->value().token();

    // The mutation is built inside the callback because the coordinator assigns
    // the timestamp, and calls back again whenever it retries.
    auto mutate_result = co_await coordinator.get().mutate(spec.s,
        token,
        [&](api::timestamp_type ts) {
            return build_mutation(spec, options, ts, json_cache, keys);
        }, timeout, qs.get_client_state().get_abort_source());

    using namespace service::strong_consistency;
    if (auto* redirect = get_if<need_redirect>(&mutate_result)) {
        bool is_write = true;
        co_return co_await redirect_statement(qp, options, redirect->target, timeout, is_write, coordinator.get().get_stats(), std::move(redirect->on_forwarding_finished));
    }
    utils::get_local_injector().inject("sc_modification_statement_timeout", [&] {
        throw exceptions::mutation_write_timeout_exception{"", "", options.get_consistency(), 0, 0, db::write_type::SIMPLE};
    });

    auto result = seastar::make_shared<result_message::void_message>();

    if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL)) {
        // Only EXECUTE requests carry a tablet version block. However,
        // QUERY requests may still target a single partition and will
        // not be rejected. We don't send any routing information for
        // them, though.
        if (options.get_tablet_version_block().has_value()) {
            auto& groups_manager = coordinator.get().get_groups_manager();
            const auto& table = spec.s->table();

            auto maybe_routing_info_v2 = groups_manager.check_tablet_version(table, token, *options.get_tablet_version_block());
            if (maybe_routing_info_v2) {
                result->add_tablet_info_v2(std::move(*maybe_routing_info_v2));
            }
        }
    }

    co_return std::move(result);
}

}
