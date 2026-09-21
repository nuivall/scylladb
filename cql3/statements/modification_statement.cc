/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#include "cql3/statements/modification_statement.hh"
#include "cql3/statements/raw/modification_statement.hh"
#include "cql3/statements/prepared_statement.hh"
#include "cql3/statements/eventual_consistency/modification_statement.hh"
#include "cql3/statements/strong_consistency/modification_statement.hh"
#include "cql3/statements/strong_consistency/statement_helpers.hh"
#include "cql3/attributes.hh"
#include "cql3/expr/evaluate.hh"
#include "cql3/expr/expr-utils.hh"
#include "cql3/result_set.hh"
#include "cql3/util.hh"
#include "data_dictionary/data_dictionary.hh"
#include "utils/assert.hh"
#include "validation.hh"

namespace cql3 {

namespace statements {

modification_statement::modification_statement(::shared_ptr<modification_spec> spec)
    : cql_statement(spec->get_timeout_config_selector())
    , _spec(std::move(spec))
{
    // The modification carries the audit info its statement kind was prepared
    // with, and a batch reads it from there. A statement is audited as itself,
    // so take a copy.
    if (const auto* ai = _spec->audit_info()) {
        set_audit_info(std::make_unique<audit::audit_info>(*ai));
    }
}

modification_statement::~modification_statement() = default;

uint32_t modification_statement::get_bound_terms() const {
    return _spec->get_bound_terms();
}

future<> modification_statement::check_access(query_processor& qp, const service::client_state& state) const {
    return _spec->check_access(state);
}

void modification_statement::validate(query_processor& qp, const service::client_state& state) const {
    _spec->validate(state);
}

bool modification_statement::depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const {
    return _spec->depends_on(ks_name, cf_name);
}

bool modification_statement::should_reclassify_control_connection() const {
    return _spec->should_reclassify_control_connection();
}

bool modification_statement::is_conditional() const {
    return _spec->has_conditions();
}

seastar::shared_ptr<const metadata> modification_statement::get_result_metadata() const {
    if (const auto& m = _spec->cas_result_metadata()) {
        return m;
    }
    return make_empty_metadata();
}

const statement_type statement_type::INSERT = statement_type(statement_type::type::insert);
const statement_type statement_type::UPDATE = statement_type(statement_type::type::update);
const statement_type statement_type::DELETE = statement_type(statement_type::type::del);
const statement_type statement_type::SELECT = statement_type(statement_type::type::select);

namespace raw {

std::unique_ptr<prepared_statement>
modification_statement::prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());
    auto meta = get_prepare_context();

    auto spec = prepare(db, meta, stats);

    auto statement = std::invoke([&] -> shared_ptr<cql_statement> {
        if (strong_consistency::is_strongly_consistent(db, schema->ks_name())) {
            return ::make_shared<strong_consistency::modification_statement>(std::move(spec));
        }
        return ::make_shared<eventual_consistency::modification_statement>(std::move(spec));
    });

    auto partition_key_bind_indices = meta.get_partition_key_bind_indexes(*schema);
    return std::make_unique<prepared_statement>(audit_info(), std::move(statement), meta, 
        std::move(partition_key_bind_indices));
}

::shared_ptr<cql3::statements::modification_spec>
modification_statement::prepare(data_dictionary::database db, prepare_context& ctx, cql_stats& stats) const {
    schema_ptr schema = validation::validate_column_family(db, keyspace(), column_family());

    auto prepared_attributes = _attrs->prepare(db, keyspace(), column_family());
    prepared_attributes->fill_prepare_context(ctx);

    auto prepared_spec = prepare_internal(db, schema, ctx, std::move(prepared_attributes), stats);
    if (strong_consistency::is_strongly_consistent(db, schema->ks_name())) {
        if (prepared_spec->requires_read()) {
            throw exceptions::invalid_request_exception("Strongly consistent updates don't support data prefetch");
        }
        if (prepared_spec->is_timestamp_set()) {
            throw exceptions::invalid_request_exception("Strongly consistent queries don't support user-provided timestamps");
        }
        // The raw IF clauses, not has_conditions(): INSERT JSON ... IF NOT
        // EXISTS never sets the flags behind has_conditions() (issue #8682).
        if (_if_not_exists || _if_exists || _conditions) {
            throw exceptions::invalid_request_exception("Strongly consistent updates don't support conditions");
        }
        // logstor needs a row marker or partition tombstone on every mutation.
        // Cell-only changes have neither and fail inside raft apply, which aborts the node.
        if (schema->logstor_enabled() && !prepared_spec->type.is_insert() && prepared_spec->has_column_operations()) {
            throw exceptions::invalid_request_exception(prepared_spec->type.is_update()
                    ? "UPDATE is not supported on logstor tables in strongly consistent keyspaces"
                    : "Deleting individual columns is not supported on logstor tables in strongly consistent keyspaces");
        }
    }

    // At this point the prepare context instance should have a list of
    // `function_call` AST nodes corresponding to non-pure functions that
    // evaluate partition key constraints.
    //
    // These calls can affect partition key ranges computation and target shard
    // selection for LWT statements.
    // For such cases we need to forward the computed execution result of the
    // function when redirecting the query execution to another shard.
    // Otherwise, it's possible that we end up bouncing indefinitely between
    // various shards when evaluating a non-deterministic function each time on
    // each shard.
    //
    // Prepare context is used to keep track of such AST nodes and also modifies
    // them to include an id, that will be used for caching the results.
    // At this point we don't yet know if it's an LWT query or not, because the
    // prepared statement object is constructed later.
    //
    // Since this cache is only meaningful for LWT queries, just clear the ids
    // if it's not a conditional statement so that the AST nodes don't
    // participate in the caching mechanism later.
    if (!prepared_spec->has_conditions()) {
        ctx.clear_pk_function_calls_cache();
    }
    prepared_spec->_may_use_token_aware_routing = ctx.get_partition_key_bind_indexes(*schema).size() != 0;
    return prepared_spec;
}

static
expr::expression
update_for_lwt_null_equality_rules(const expr::expression& e) {
    using namespace expr;

    return search_and_replace(e, [] (const expression& e) -> std::optional<expression> {
        if (auto* binop = as_if<binary_operator>(&e)) {
            auto new_binop = *binop;
            new_binop.null_handling = expr::null_handling_style::lwt_nulls;
            return new_binop;
        }
        return std::nullopt;
    });
}

static
expr::expression
column_condition_prepare(const expr::expression& expr, data_dictionary::database db, const sstring& keyspace, const schema& schema, const dialect& d){
    auto prepared = expr::prepare_expression_allowing_relations(expr, db, keyspace, &schema, make_lw_shared<column_specification>("", "", make_shared<column_identifier>("IF condition", true), boolean_type), d);
    expr::verify_no_aggregate_functions(prepared, "IF clause");

    expr::for_each_expression<expr::column_value>(prepared, [] (const expr::column_value& cval) {
      auto def = cval.col;
      if (def->is_primary_key()) {
        throw exceptions::invalid_request_exception(format("PRIMARY KEY column '{}' cannot have IF conditions", def->name_as_text()));
      }
    });

    // If a collection is multi-cell and not frozen, it is returned as a map even if the
    // underlying data type is "set" or "list". This is controlled by
    // partition_slice::collections_as_maps enum, which is set when preparing a read command
    // object. Representing a list as a map<timeuuid, listval> is necessary to identify the list field
    // being updated, e.g. in case of UPDATE t SET list[3] = null WHERE a = 1 IF list[3]
    // = 'key'
    //
    // We adjust for it by reinterpreting the returned value as a list, since the map
    // representation is not needed here.
    prepared = expr::adjust_for_collection_as_maps(prepared);

    prepared = expr::optimize_like(prepared);

    prepared = update_for_lwt_null_equality_rules(prepared);


    return prepared;
}


void
modification_statement::prepare_conditions(data_dictionary::database db, const schema& schema, prepare_context& ctx,
        cql3::statements::modification_spec& spec) const
{
    if (_if_not_exists || _if_exists || _conditions) {
        if (spec.is_counter()) {
            throw exceptions::invalid_request_exception("Conditional updates are not supported on counter tables");
        }
        if (_attrs->timestamp) {
            throw exceptions::invalid_request_exception("Cannot provide custom timestamp for conditional updates");
        }

        if (_if_not_exists) {
            // To have both 'IF NOT EXISTS' and some other conditions doesn't make sense.
            // So far this is enforced by the parser, but let's throwing_assert it for sanity if ever the parse changes.
            throwing_assert(!_conditions);
            throwing_assert(!_if_exists);
            spec.set_if_not_exist_condition();
        } else if (_if_exists) {
            throwing_assert(!_conditions);
            throwing_assert(!_if_not_exists);
            spec.set_if_exist_condition();
        } else {
            spec._condition = column_condition_prepare(*_conditions, db, keyspace(), schema, ctx.get_dialect());
            expr::fill_prepare_context(spec._condition, ctx);
            spec.analyze_condition(spec._condition);
        }
        spec.build_cas_result_set_metadata();
    }
}

audit::statement_category modification_statement::category() const {
    return audit::statement_category::DML;
}

modification_statement::modification_statement(cf_name name, std::unique_ptr<attributes::raw> attrs, std::optional<expr::expression> conditions, bool if_not_exists, bool if_exists)
    : cf_statement{std::move(name)}
    , _attrs{std::move(attrs)}
    , _conditions{std::move(conditions)}
    , _if_not_exists{if_not_exists}
    , _if_exists{if_exists}
{ }

}  // namespace raw

}

}
