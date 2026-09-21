/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/eventual_consistency/modification_statement.hh"

#include "cql3/attributes.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/cas_request.hh"
#include "cql3/util.hh"
#include "db/consistency_level_validations.hh"
#include "db/large_data_handler.hh"
#include "replica/database.hh"
#include "service/storage_proxy.hh"
#include "transport/cql_protocol_extension.hh"
#include "transport/messages/result_message.hh"
#include "utils/error_injection.hh"

#include <seastar/core/execution_stage.hh>

#include <boost/lexical_cast.hpp>

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

namespace cql3::statements::eventual_consistency {

using result_message = cql_transport::messages::result_message;

static lw_shared_ptr<query::read_command>
read_command(const modification_spec& spec, query_processor& qp, query::clustering_row_ranges ranges, db::consistency_level cl) {
    try {
        validate_for_read(cl);
    } catch (exceptions::invalid_request_exception& e) {
        throw exceptions::invalid_request_exception(format("Write operation require a read but consistency {} is not supported on reads", cl));
    }
    query::partition_slice ps(std::move(ranges), *spec.s, spec.columns_to_read(), update_parameters::options);
    const auto max_result_size = qp.proxy().get_max_result_size(ps);
    return make_lw_shared<query::read_command>(spec.s->id(), spec.s->version(), std::move(ps), query::max_result_size(max_result_size), query::tombstone_limit::max);
}

future<utils::chunked_vector<mutation>>
get_mutations(const modification_spec& spec, query_processor& qp, const query_options& options,
        db::timeout_clock::time_point timeout, bool local, int64_t now, service::query_state& qs,
        modification_spec::json_cache_opt& json_cache, std::vector<dht::partition_range> keys) {
    auto cl = options.get_consistency();
    auto ranges = spec.create_clustering_ranges(options, json_cache);
    auto f = make_ready_future<update_parameters::prefetch_data>(spec.s);

    if (spec.is_counter()) {
        db::validate_counter_for_write(*spec.s, cl);
    } else {
        db::validate_for_write(cl);
    }

    if (spec.requires_read()) {
        lw_shared_ptr<query::read_command> cmd = read_command(spec, qp, ranges, cl);
        // FIXME: ignoring "local"
        f = qp.proxy().query(spec.s, cmd, dht::partition_range_vector(keys), cl,
                {timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()}).then(

                [&spec, cmd] (auto cqr) {

            return update_parameters::build_prefetch_data(spec.s, *cqr.query_result, cmd->slice);
        });
    }

    return f.then([&spec, keys = std::move(keys), ranges = std::move(ranges), json_cache = std::move(json_cache), &options, now]
            (auto rows) {
        return spec.build_mutations(options, spec.get_timestamp(now, options), keys, ranges, json_cache, std::move(rows));
    });
}

struct modification_statement_executor {
    static auto get() { return &modification_statement::do_execute; }
};
static thread_local inheriting_concrete_execution_stage<
        future<::shared_ptr<result_message>>,
        const modification_statement*,
        query_processor&,
        service::query_state&,
        const query_options&> modify_stage{"cql3_modification", modification_statement_executor::get()};

future<::shared_ptr<result_message>>
modification_statement::execute(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const {
    return execute_without_checking_exception_message(qp, qs, options, std::move(guard))
            .then(cql_transport::messages::propagate_exception_as_future<shared_ptr<result_message>>);
}

future<::shared_ptr<result_message>>
modification_statement::execute_without_checking_exception_message(query_processor& qp, service::query_state& qs, const query_options& options, std::optional<service::group0_guard> guard) const {
    cql3::util::validate_timestamp(qp.get_cql_config(), options, spec().attrs);
    return modify_stage(this, seastar::ref(qp), seastar::ref(qs), seastar::cref(options));
}

future<::shared_ptr<result_message>>
modification_statement::do_execute(query_processor& qp, service::query_state& qs, const query_options& options) const {
    const modification_spec& spec = this->spec();

    if (!qp.db().try_find_table(spec.s->id())) {
        co_return coroutine::exception(
                std::make_exception_ptr(exceptions::invalid_request_exception(
                        format("unconfigured table {}", spec.column_family()))));
    }

    tracing::add_table_name(qs.get_trace_state(), spec.keyspace(), spec.column_family());

    spec.inc_cql_stats(qs.get_client_state().is_internal());

    const auto cl = options.get_consistency();
    const query_processor::write_consistency_guardrail_state guardrail_state = qp.check_write_consistency_levels_guardrail(cl);
    if (guardrail_state == query_processor::write_consistency_guardrail_state::FAIL) {
        co_return coroutine::exception(
                std::make_exception_ptr(exceptions::invalid_request_exception(
                        format("Write consistency level {} is forbidden by the current configuration "
                               "setting of write_consistency_levels_disallowed. Please use a different "
                               "consistency level, or remove {} from write_consistency_levels_disallowed "
                               "set in the configuration.", cl, cl))));
    }

    spec.validate_primary_key(options);

    if (spec.has_conditions()) {
        auto result = co_await execute_with_condition(qp, qs, options);
        if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
            result->add_warning(format("Using write consistency level {} listed on the "
                                       "write_consistency_levels_warned is not recommended.", cl));
        }
        co_return result;
    }

    modification_spec::json_cache_opt json_cache = spec.maybe_prepare_json_cache(options);
    std::vector<dht::partition_range> keys = spec.build_partition_keys(options, json_cache);

    bool keys_size_one = keys.size() == 1;
    auto token = dht::token();
    if (keys_size_one) {
        token = keys[0].start()->value().token();
    } 

    auto violations = db::large_data_violation_type::none;
    auto res = co_await execute_without_condition(qp, qs, options, json_cache, std::move(keys), &violations);
    
    if (!res) {
        co_return seastar::make_shared<result_message::exception>(std::move(res).assume_error());
    }

    auto result = seastar::make_shared<result_message::void_message>();
    if (guardrail_state == query_processor::write_consistency_guardrail_state::WARN) {
        result->add_warning(format("Using write consistency level {} listed on the "
                                   "write_consistency_levels_warned is not recommended.", cl));
    }
    // Surface any coordinator-side large data guardrail soft limit violations
    // detected during the write to the client as a CQL warning.
    if (auto warning = db::large_data_soft_violation_warning(violations); !warning.empty()) [[unlikely]] {
        result->add_warning(std::move(warning));
    }

    auto&& table = spec.s->table();

    if (keys_size_one && spec._may_use_token_aware_routing && table.uses_tablets()) {
        auto erm = table.get_effective_replication_map();
        if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V2_EXPERIMENTAL)) {
            // We only return routing information for EXECUTE requests.
            // They will carry a tablet version block; QUERY reqeuests
            // will not.
            if (options.get_tablet_version_block().has_value()) {
                auto tablet_info_v2 = erm->check_tablet_version(token, *options.get_tablet_version_block());
                if (tablet_info_v2) {
                    result->add_tablet_info_v2(std::move(*tablet_info_v2));
                }
            }
        } else if (qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
            auto tablet_info = erm->check_locality(token, qs.get_client_state().get_original_shard());
            if (tablet_info.has_value()) {
                result->add_tablet_info(std::move(*tablet_info));
            }
        }
    }

    co_return std::move(result);
}

future<coordinator_result<>>
modification_statement::execute_without_condition(query_processor& qp, service::query_state& qs, const query_options& options,
        modification_spec::json_cache_opt& json_cache, std::vector<dht::partition_range> keys,
        db::large_data_violation_type* violations) const {
    auto cl = options.get_consistency();
    auto timeout = db::timeout_clock::now() + spec().get_timeout(qs.get_client_state(), options);
    return get_mutations(spec(), qp, options, timeout, false, options.get_timestamp(qs), qs, json_cache, std::move(keys)).then([this, cl, timeout, &qp, &qs, &options, violations] (auto mutations) {
        if (mutations.empty()) {
            return make_ready_future<coordinator_result<>>(bo::success());
        }

        return qp.proxy().mutate_with_triggers(std::move(mutations), cl, timeout, false, qs.get_trace_state(), qs.get_permit(), db::allow_per_partition_rate_limit::yes, spec().is_raw_counter_shard_write(), {
            .node_local_only = options.get_specific_options().node_local_only,
            .bypass_large_data_guardrails = spec().attrs->is_bypass_large_data_guardrails(),
            .violations_out = violations
        });
    });
}

namespace {

future<::shared_ptr<result_message>>
process_forced_rebounce(unsigned shard, query_processor& qp, const query_options& options) {
    static int64_t counter = {0};
    static logging::logger logger("modification_statement");
    if (counter <= 0) {
        const auto counter_opt = utils::get_local_injector().inject_parameter<decltype(counter)>("forced_bounce_to_shard_counter");
        decltype(counter) counter_value = 0;
        if (!counter_opt) {
            logger.warn("forced_bounce_to_shard_counter is not set. Using default value 1.");
        } else {
            try {
                counter_value = boost::lexical_cast<decltype(counter_value)>(*counter_opt);
            } catch (const boost::bad_lexical_cast& e) {
                logger.warn("Incorrect forced_bounce_to_shard_counter value: [{}]. Using default value 1.", *counter_opt);
            }
        }
        if (counter_value <= 0) {
            counter_value = 1;
        }
        counter = counter_value;
    }

    const auto prev_counter_value = counter;
    if (prev_counter_value <= 1) {
        logger.info("Disabling forced_bounce_to_shard_counter.");
        co_await utils::error_injection_type::disable_on_all("forced_bounce_to_shard_counter");
        counter = 0;
    } else {
        --counter;
    }

    // While counter > 1 select a different shard to re-bounce to.
    // On the last iteration, re-bounce to the correct shard.
    if (counter != 0) {
        const auto shard_num = this_smp_shard_count();
        const auto local_shard = this_shard_id();
        auto target_shard = local_shard + 1;
        if (target_shard == shard) {
            ++target_shard;
        }
        if (target_shard > shard_num - 1) {
            target_shard = 0;
        }
        shard = target_shard;
    }

    logger.info("Applying forced_bounce_to_shard_counter, re-bouncing to shard {}.", shard);
    co_return co_await make_ready_future<shared_ptr<result_message>>(
        qp.bounce_to_shard(shard, std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls())));
}

} // namespace

future<::shared_ptr<result_message>>
modification_statement::execute_with_condition(query_processor& qp, service::query_state& qs, const query_options& options) const {
    const modification_spec& spec = this->spec();

    auto cl_for_learn = options.get_consistency();
    utils::result_with_exception_ptr<db::consistency_level> cl_for_paxos = options.check_serial_consistency();
    if (!cl_for_paxos) [[unlikely]] {
        return make_exception_future<shared_ptr<result_message>>(std::move(cl_for_paxos).assume_error());
    }
    db::timeout_clock::time_point now = db::timeout_clock::now();
    const timeout_config& cfg = qs.get_client_state().get_timeout_config();

    auto statement_timeout = now + cfg.write_timeout; // All CAS networking operations run with write timeout.
    auto cas_timeout = now + cfg.cas_timeout;         // When to give up due to contention.
    auto read_timeout = now + cfg.read_timeout;       // When to give up on query.

    modification_spec::json_cache_opt json_cache = spec.maybe_prepare_json_cache(options);
    std::vector<dht::partition_range> keys = spec.build_partition_keys(options, json_cache);
    std::vector<query::clustering_range> ranges = spec.create_clustering_ranges(options, json_cache);

    if (keys.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted partition key in a conditional {}",
                    spec.type.is_update() ? "update" : "deletion"));
    }
    if (ranges.empty()) {
        throw exceptions::invalid_request_exception(format("Unrestricted clustering key in a conditional {}",
                    spec.type.is_update() ? "update" : "deletion"));
    }

    auto request = std::make_unique<cas_request>(spec.s, std::move(keys));
    auto* request_ptr = request.get();
    // cas_request can be used for batches as well single statements; Here we have just a single
    // modification in the list of CAS commands, since we're handling single-statement execution.
    request->add_row_update(spec, std::move(ranges), std::move(json_cache), options);

    auto token = request->key()[0].start()->value().as_decorated_key().token();

    auto cas_shard = service::cas_shard(*spec.s, token);

    if (utils::get_local_injector().is_enabled("forced_bounce_to_shard_counter")) {
        return process_forced_rebounce(cas_shard.shard(), qp, options);
    }
    if (!cas_shard.this_shard()) {
        return make_ready_future<shared_ptr<result_message>>(
                qp.bounce_to_shard(cas_shard.shard(), std::move(const_cast<cql3::query_options&>(options).take_cached_pk_function_calls()))
            );
    }

    std::optional<locator::tablet_routing_info> tablet_info;

    auto&& table = spec.s->table();
    if (spec._may_use_token_aware_routing && table.uses_tablets() && qs.get_client_state().is_protocol_extension_set(cql_transport::cql_protocol_extension::TABLETS_ROUTING_V1)) {
        auto erm = table.get_effective_replication_map();
        tablet_info = erm->check_locality(token, qs.get_client_state().get_original_shard());
    }

    return qp.proxy().cas(spec.s, std::move(cas_shard), *request_ptr, request->read_command(qp), request->key(),
            {read_timeout, qs.get_permit(), qs.get_client_state(), qs.get_trace_state()},
            std::move(cl_for_paxos).assume_value(), cl_for_learn, statement_timeout, cas_timeout, true, {},
            spec.attrs->is_bypass_large_data_guardrails()).then([this, request = std::move(request), tablet_info = std::move(tablet_info)] (service::storage_proxy::cas_result cas_result) mutable {
        auto result = request->build_cas_result_set(spec().cas_result_metadata(), spec().columns_of_cas_result_set(), cas_result.is_applied);
        if (tablet_info) {
            result->add_tablet_info(std::move(*tablet_info));
        }
        // Surface any coordinator-side large data guardrail soft limit violations
        // detected during the LWT to the client as a CQL warning.
        if (auto warning = db::large_data_soft_violation_warning(cas_result.large_data_violations); !warning.empty()) [[unlikely]] {
            result->add_warning(std::move(warning));
        }
        return result;
    });
}

}
