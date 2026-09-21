/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "batch_statement.hh"
#include "cql3/util.hh"
#include "raw/batch_statement.hh"
#include "cql3/cql_config.hh"
#include "db/consistency_level_validations.hh"
#include "data_dictionary/data_dictionary.hh"
#include <ranges>
#include "cas_request.hh"
#include "cql3/query_processor.hh"
#include "tracing/trace_state.hh"
#include "utils/unique_view.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "cql3/statements/strong_consistency/batch_statement.hh"
#include "cql3/statements/eventual_consistency/batch_statement.hh"

template<typename T = void>
using coordinator_result = exceptions::coordinator_result<T>;

namespace cql3 {

namespace statements {

timeout_config_selector
timeout_for_type(batch_statement::type t) {
    return t == batch_statement::type::COUNTER
            ? &timeout_config::counter_write_timeout
            : &timeout_config::write_timeout;
}

db::timeout_clock::duration batch_statement::get_timeout(const service::client_state& state, const query_options& options) const {
    return _attrs->is_timeout_set() ? _attrs->get_timeout(options) : state.get_timeout_config().*get_timeout_config_selector();
}

batch_statement::batch_statement(int bound_terms, type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : cql_statement(timeout_for_type(type_))
    , _bound_terms(bound_terms), _type(type_), _statements(std::move(statements))
    , _attrs(std::move(attrs))
    , _has_conditions(std::ranges::any_of(_statements, [] (auto&& s) { return s.spec->has_conditions(); }))
    , _stats(stats)
{
    // Deliberately no validate() here: what a batch may contain depends on how
    // it is committed, so each sub-class validates in its own constructor,
    // where the checks it needs are known.
}

batch_statement::batch_statement(type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs), stats)
{
}

bool batch_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const
{
    return std::ranges::any_of(_statements, [&ks_name, &cf_name] (auto&& s) { return s.spec->depends_on(ks_name, cf_name); });
}

uint32_t batch_statement::get_bound_terms() const
{
    return _bound_terms;
}

future<> batch_statement::check_access(query_processor& qp, const service::client_state& state) const
{
    return parallel_for_each(_statements.begin(), _statements.end(), [&qp, &state](auto&& s) {
        if (s.needs_authorization) {
            return s.spec->check_access(state);
        } else {
            return make_ready_future<>();
        }
    });
}

void batch_statement::validate()
{
    if (_attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Global TTL on the BATCH statement is not supported.");
    }

    bool timestamp_set = _attrs->is_timestamp_set();
    if (timestamp_set) {
        if (_has_conditions) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional BATCH");
        }
        if (_type == type::COUNTER) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter BATCH");
        }
    }

    bool has_counters = std::ranges::any_of(_statements, [] (auto&& s) { return s.spec->is_counter(); });
    bool has_non_counters = !std::ranges::all_of(_statements, [] (auto&& s) { return s.spec->is_counter(); });
    if (timestamp_set && has_counters) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for a BATCH containing counters");
    }
    if (timestamp_set && std::ranges::any_of(_statements, [] (auto&& s) { return s.spec->is_timestamp_set(); })) {
        throw exceptions::invalid_request_exception("Timestamp must be set either on BATCH or individual statements");
    }
    if (_type == type::COUNTER && has_non_counters) {
        throw exceptions::invalid_request_exception("Cannot include non-counter statement in a counter batch");
    }
    if (_type == type::LOGGED && has_counters) {
        throw exceptions::invalid_request_exception("Cannot include a counter statement in a logged batch");
    }
    if (has_counters && has_non_counters) {
        throw exceptions::invalid_request_exception("Counter and non-counter mutations cannot exist in the same batch");
    }

    if (_has_conditions
            && !_statements.empty()
            && (std::ranges::distance(_statements
                            | std::views::transform([] (auto&& s) { return s.spec->keyspace(); })
                            | utils::views::unique) != 1
                || (std::ranges::distance(_statements
                        | std::views::transform([] (auto&& s) { return s.spec->column_family(); })
                        | utils::views::unique) != 1))) {
        throw exceptions::invalid_request_exception("BATCH with conditions cannot span multiple tables");
    }
    std::optional<bool> raw_counter;
    for (auto& s : _statements) {
        if (raw_counter && s.spec->is_raw_counter_shard_write() != *raw_counter) {
            throw exceptions::invalid_request_exception("Cannot mix raw and regular counter statements in batch");
        }
        raw_counter = s.spec->is_raw_counter_shard_write();
    }
}

void batch_statement::validate(query_processor& qp, const service::client_state& state) const
{
    for (auto&& s : _statements) {
        s.spec->validate(state);
    }
}

const std::vector<batch_statement::single_statement>& batch_statement::get_statements() const
{
    return _statements;
}

void batch_statement::build_cas_result_set_metadata() {
    if (_statements.empty()) {
        return;
    }
    const auto& schema = *_statements.front().spec->s;

    _columns_of_cas_result_set.resize(schema.all_columns_count());

    // Add the mandatory [applied] column to result set metadata
    std::vector<lw_shared_ptr<column_specification>> columns;

    auto applied = make_lw_shared<cql3::column_specification>(schema.ks_name(), schema.cf_name(),
            ::make_shared<cql3::column_identifier>("[applied]", false), boolean_type);
    columns.push_back(applied);

    for (const auto& def : schema.primary_key_columns()) {
        _columns_of_cas_result_set.set(def.ordinal_id);
    }
    for (const auto& s : _statements) {
        _columns_of_cas_result_set.union_with(s.spec->columns_of_cas_result_set());
    }
    columns.reserve(_columns_of_cas_result_set.count());
    for (const auto& def : schema.all_columns()) {
        if (_columns_of_cas_result_set.test(def.ordinal_id)) {
            columns.emplace_back(def.column_specification);
        }
    }
    _metadata = seastar::make_shared<cql3::metadata>(std::move(columns));
}

namespace raw {

std::unique_ptr<prepared_statement>
batch_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    auto&& meta = get_prepare_context();

    std::optional<sstring> first_ks;
    std::optional<sstring> first_cf;
    bool have_multiple_cfs = false;

    std::vector<cql3::statements::batch_statement::single_statement> statements;
    statements.reserve(_parsed_statements.size());
    std::vector<std::reference_wrapper<const audit::audit_info>> batch_audit_infos;
    batch_audit_infos.reserve(_parsed_statements.size());

    bool has_sc_statements = false;
    bool has_non_sc_statements = false;
    for (auto&& parsed : _parsed_statements) {
        if (!first_ks) {
            first_ks = parsed->keyspace();
            first_cf = parsed->column_family();
        } else {
            have_multiple_cfs |= first_ks.value() != parsed->keyspace();
            have_multiple_cfs |= first_cf.value() != parsed->column_family();
        }
        auto spec = parsed->prepare(db, meta, stats);
        if (strong_consistency::is_strongly_consistent(db, parsed->keyspace())) {
            has_sc_statements = true;
        } else {
            has_non_sc_statements = true;
        }
        if (auto* audit_info = spec->audit_info()) {
            audit_info->set_query_string(parsed->get_raw_cql());
            batch_audit_infos.emplace_back(*audit_info);
        }
        statements.emplace_back(std::move(spec));
    }
    if (has_sc_statements && has_non_sc_statements) {
        throw exceptions::invalid_request_exception("Cannot mix strongly consistent and eventually consistent statements in a batch");
    }

    auto&& prep_attrs = _attrs->prepare(db, "[batch]", "[batch]");
    prep_attrs->fill_prepare_context(meta);

    std::vector<uint16_t> partition_key_bind_indices;
    if (!have_multiple_cfs && !statements.empty()) {
        partition_key_bind_indices = meta.get_partition_key_bind_indexes(*statements[0].spec->s);
    }

    shared_ptr<cql_statement> statement;
    if (has_sc_statements) {
        statement = ::make_shared<strong_consistency::batch_statement>(meta.bound_variables_size(), _type, std::move(statements), std::move(prep_attrs));
    } else {
        statement = ::make_shared<eventual_consistency::batch_statement>(meta.bound_variables_size(), _type, std::move(statements), std::move(prep_attrs), stats);
    }

    auto ai = audit_info();
    if (ai) {
        ai->set_batch_infos(std::move(batch_audit_infos));
    }

    return std::make_unique<prepared_statement>(std::move(ai), std::move(statement),
                                                      meta.get_variable_specifications(),
                                                      std::move(partition_key_bind_indices));
}

audit::statement_category batch_statement::category() const {
    return audit::statement_category::DML;
}

}


}

}


