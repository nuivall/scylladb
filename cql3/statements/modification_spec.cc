/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "utils/assert.hh"
#include "cql3/statements/modification_spec.hh"
#include "cql3/attributes.hh"
#include "cql3/operation.hh"
#include "cql3/query_processor.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/expr/evaluate.hh"
#include "data_dictionary/data_dictionary.hh"
#include "types/collection.hh"

#include <optional>

bool is_internal_keyspace(std::string_view name);

namespace cql3 {

namespace statements {

timeout_config_selector
modification_timeout(const schema& s) {
    if (s.is_counter()) {
        return &timeout_config::counter_write_timeout;
    } else {
        return &timeout_config::write_timeout;
    }
}

db::timeout_clock::duration modification_spec::get_timeout(const service::client_state& state, const query_options& options) const {
    return attrs->is_timeout_set() ? attrs->get_timeout(options) : state.get_timeout_config().*get_timeout_config_selector();
}

modification_spec::modification_spec(statement_type type_, uint32_t bound_terms,
        schema_ptr schema_, std::unique_ptr<attributes> attrs_, cql_stats& stats_)
    : type{type_}
    , _bound_terms{bound_terms}
    , s{schema_}
    , attrs{std::move(attrs_)}
    , _stats(stats_)
    , _columns_to_read(schema_->all_columns_count())
    , _columns_of_cas_result_set(schema_->all_columns_count())
    , _column_operations{}
    , _ks_sel(::is_internal_keyspace(schema_->ks_name()) ? ks_selector::SYSTEM : ks_selector::NONSYSTEM)
    , _timeout_config_selector(modification_timeout(*schema_))
{ }

modification_spec::~modification_spec() = default;

uint32_t modification_spec::get_bound_terms() const {
    return _bound_terms;
}

const sstring& modification_spec::keyspace() const {
    return s->ks_name();
}

bool modification_spec::should_reclassify_control_connection() const {
    // A control connection legitimately writes only to system tables; writing any
    // other keyspace means it is being used for user load.
    return _ks_sel == ks_selector::NONSYSTEM;
}

const sstring& modification_spec::column_family() const {
    return s->cf_name();
}

bool modification_spec::is_counter() const {
    return s->is_counter();
}

bool modification_spec::is_view() const {
    return s->is_view();
}

int64_t modification_spec::get_timestamp(int64_t now, const query_options& options) const {
    return attrs->get_timestamp(now, options);
}

bool modification_spec::is_timestamp_set() const {
    return attrs->is_timestamp_set();
}

std::optional<gc_clock::duration> modification_spec::get_time_to_live(const query_options& options) const {
    std::optional<int32_t> ttl = attrs->get_time_to_live(options);
    return ttl ? std::make_optional<gc_clock::duration>(*ttl) : std::nullopt;
}

future<> modification_spec::check_access(query_processor& qp, const service::client_state& state) const {
    auto f = state.has_column_family_access(keyspace(), column_family(), auth::permission::MODIFY);
    if (has_conditions()) {
        f = f.then([this, &state] {
           return state.has_column_family_access(keyspace(), column_family(), auth::permission::SELECT);
        });
    }
    return f;
}

bool modification_spec::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return keyspace() == ks_name && (!cf_name || column_family() == *cf_name);
}

void modification_spec::inc_cql_stats(bool is_internal) const {
    const source_selector src_sel = is_internal
            ? source_selector::INTERNAL : source_selector::USER;
    const cond_selector cond_sel = has_conditions()
            ? cond_selector::WITH_CONDITIONS : cond_selector::NO_CONDITIONS;
    ++_stats.query_cnt(src_sel, _ks_sel, cond_sel, type);
}

bool modification_spec::applies_to(const selection::selection* selection,
        const update_parameters::prefetch_data::row* row,
        const query_options& options) const {

    // Assume the row doesn't exist if it has no static columns and the statement is only interested
    // in static column values. Needed for EXISTS checks to work correctly. For example, the following
    // conditional INSERT must apply, because there's no static row in the partition although there's
    // a regular row, which is fetched by the read:
    //   CREATE TABLE t(p int, c int, s int static, PRIMARY KEY(p, c));
    //   INSERT INTO t(p, c) VALUES(1, 1);
    //   INSERT INTO t(p, s) VALUES(1, 1) IF NOT EXISTS;
    if (has_only_static_column_conditions() && row && !row->has_static_columns(*s)) {
        row = nullptr;
    }

    if (_if_exists) {
        return row != nullptr;
    }
    if (_if_not_exists) {
        return row == nullptr;
    }

    // Fake out an all-null static_and_regular_columns if we didn't find a row
    auto fake_static_and_regular_columns = std::vector<managed_bytes_opt>();
    auto static_and_regular_columns = std::invoke([&] () -> const std::vector<managed_bytes_opt>* {
        if (row) {
            return &row->cells;
        } else {
            fake_static_and_regular_columns.resize(selection->get_column_count());
            return &fake_static_and_regular_columns;
        }
    });

    auto inputs = expr::evaluation_inputs{
        .static_and_regular_columns = *static_and_regular_columns,
        .selection = selection,
        .options = &options,
    };

    static auto true_value = raw_value::make_value(data_value(true).serialize());
    return expr::evaluate(_condition, inputs) == true_value;
}

void modification_spec::classify_exists_condition(bool restricts_clustering_columns) {
    /*
     * If there's no clustering columns restriction, we may assume that EXISTS
     * check only selects static columns and hence we can use any row from the
     * partition to check conditions.
     */
    if (_if_exists || _if_not_exists) {
        throwing_assert(!_has_static_column_conditions && !_has_regular_column_conditions);
        if (s->has_static_columns() && !restricts_clustering_columns) {
            _has_static_column_conditions = true;
        } else {
            _has_regular_column_conditions = true;
        }
    }
}

void modification_spec::analyze_condition(expr::expression cond) {
  expr::for_each_expression<expr::column_value>(cond, [&] (const expr::column_value& col) {
    if (col.col->is_static()) {
        _has_static_column_conditions = true;
    } else {
        _has_regular_column_conditions = true;
    }
  });
}

void modification_spec::set_if_not_exist_condition() {
    // We don't know yet if we need to select only static columns to check this
    // condition or we need regular columns as well. So we postpone setting
    // _has_regular_column_conditions/_has_static_column_conditions flag until
    // we process WHERE clause, see process_where_clause().
    _if_not_exists = true;
}

bool modification_spec::has_if_not_exist_condition() const {
    return _if_not_exists;
}

void modification_spec::set_if_exist_condition() {
    // See a comment in set_if_not_exist_condition().
    _if_exists = true;
}

bool modification_spec::has_if_exist_condition() const {
    return _if_exists;
}
void modification_spec::build_cas_result_set_metadata() {

    std::vector<lw_shared_ptr<column_specification>> columns;
    // Add the mandatory [applied] column to result set metadata
    auto applied = make_lw_shared<cql3::column_specification>(s->ks_name(), s->cf_name(),
            make_shared<cql3::column_identifier>("[applied]", false), boolean_type);

    columns.push_back(applied);

    const auto& all_columns = s->all_columns();
    if (_if_exists || _if_not_exists) {
        // If all our conditions are columns conditions (IF x = ?), then it's enough to query
        // the columns from the conditions. If we have a IF EXISTS or IF NOT EXISTS however,
        // we need to query all columns for the row since if the condition fails, we want to
        // return everything to the user.
        // XXX Static columns make this a bit more complex, in that if an insert only static
        // columns, then the existence condition applies only to the static columns themselves, and
        // so we don't want to include regular columns in that case.
        for (const auto& def : all_columns) {
            _columns_of_cas_result_set.set(def.ordinal_id);
        }
    } else {
        expr::for_each_expression<expr::column_value>(_condition, [&] (const expr::column_value& col) {
            _columns_of_cas_result_set.set(col.col->ordinal_id);
        });
    }
    columns.reserve(columns.size() + all_columns.size());
    // We must filter conditions using the _columns_of_cas_result_set, since
    // the same column can be used twice in the condition list:
    // if a > 0 and a < 3.
    for (const auto& def : all_columns) {
        if (_columns_of_cas_result_set.test(def.ordinal_id)) {
            columns.emplace_back(def.column_specification);
        }
    }
    // Ensure we prefetch all of the columns of the result set. This is also
    // necessary to check conditions.
    _columns_to_read.union_with(_columns_of_cas_result_set);
    _cas_result_metadata = seastar::make_shared<cql3::metadata>(std::move(columns));
}

modification_spec::validate(query_processor&, const service::client_state& state) const {
    if (has_conditions() && attrs->is_timestamp_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
    }

    if (is_counter() && attrs->is_timestamp_set() && !is_raw_counter_shard_write()) {
        throw exceptions::invalid_request_exception("Cannot provide custom timestamp for counter updates");
    }

    if (is_counter() && attrs->is_time_to_live_set()) {
        throw exceptions::invalid_request_exception("Cannot provide custom TTL for counter updates");
    }

    if (is_view()) {
        throw exceptions::invalid_request_exception("Cannot directly modify a materialized view");
    }
}

void modification_spec::add_operation(std::unique_ptr<operation> op) {
    if (op->column.is_static()) {
        _sets_static_columns = true;
    } else {
        _sets_regular_columns = true;
    }
    if (op->requires_read()) {
        _requires_read = true;
        _columns_to_read.set(op->column.ordinal_id);
        if (op->column.type->is_collection() ) {
            auto ctype = static_pointer_cast<const collection_type_impl>(op->column.type);
            if (!ctype->is_multi_cell()) {
                throw std::logic_error(format("cannot prefetch frozen collection: {}", op->column.name_as_text()));
            }
        }
    }

    if (op->requires_lwt()) {
        _requires_lwt = true;
    }

    if (op->column.is_counter()) {
        auto is_raw_counter_shard_write = op->is_raw_counter_shard_write();
        if (_is_raw_counter_shard_write && _is_raw_counter_shard_write != is_raw_counter_shard_write) {
            throw exceptions::invalid_request_exception("Cannot mix regular and raw counter updates");
        }
        _is_raw_counter_shard_write = is_raw_counter_shard_write;
    }

    _column_operations.push_back(std::move(op));
}

void modification_spec::reject_in_relations_with_conditions(bool key_is_in_relation, bool clustering_key_has_IN) const {
    // We don't support IN for CAS operation so far
    if (key_is_in_relation) {
        throw exceptions::invalid_request_exception(
                format("IN on the partition key is not supported with conditional {}",
                    type.is_update() ? "updates" : "deletions"));
    }

    if (clustering_key_has_IN) {
        throw exceptions::invalid_request_exception(
                format("IN on the clustering key columns is not supported with conditional {}",
                    type.is_update() ? "updates" : "deletions"));
    }
}

}

}
