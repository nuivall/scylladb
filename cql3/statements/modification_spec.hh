/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/stats.hh"
#include "cql3/update_parameters.hh"
#include "cql3/statements/statement_type.hh"
#include "db/timeout_clock.hh"
#include "timeout_config.hh"

#include <seastar/core/shared_ptr.hh>

#include <memory>
#include <optional>

namespace service {
class client_state;
class query_state;
}

namespace cql3 {

class query_processor;
class query_options;
class attributes;
class operation;

namespace statements {

namespace raw { class modification_statement; }

// Which of the client's write timeouts applies to a modification of this table.
timeout_config_selector modification_timeout(const schema& s);

/*
 * Everything a single modification - an INSERT, an UPDATE or a DELETE - knows
 * about itself after it has been parsed: the table it addresses, the attributes
 * and conditions it carries, and the mutation it turns into.
 *
 * Deliberately not a cql_statement. Turning CQL into a mutation is the same
 * whether the mutation is then committed through storage_proxy or through Raft,
 * so this class is shared by both and knows nothing about either.
 */
class modification_spec {
public:
    const statement_type type;
private:
    const uint32_t _bound_terms;
public:
    const schema_ptr s;
    const std::unique_ptr<attributes> attrs;

protected:
    cql_stats& _stats;

    expr::expression _condition = expr::conjunction{{}}; // TRUE
private:
    const ks_selector _ks_sel;
    // Which of the client's write timeouts applies to this modification. The
    // statement wrapping the spec passes it on to cql_statement.
    const timeout_config_selector _timeout_config_selector;

    // True if this statement has _if_exists or _if_not_exists or other
    // conditions that apply to static/regular columns, respectively.
    // Pre-computed during statement prepare.
    bool _has_static_column_conditions = false;
    bool _has_regular_column_conditions = false;
    bool _if_not_exists = false;
    bool _if_exists = false;

public:
    modification_spec(
            statement_type type_,
            uint32_t bound_terms,
            schema_ptr schema_,
            std::unique_ptr<attributes> attrs_,
            cql_stats& stats_);

    virtual ~modification_spec();

    uint32_t get_bound_terms() const;

    const sstring& keyspace() const;

    const sstring& column_family() const;

    bool is_counter() const;

    bool is_view() const;

    int64_t get_timestamp(int64_t now, const query_options& options) const;

    bool is_timestamp_set() const;

    std::optional<gc_clock::duration> get_time_to_live(const query_options& options) const;

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;

    future<> check_access(query_processor& qp, const service::client_state& state) const;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const;

    bool should_reclassify_control_connection() const;

    timeout_config_selector get_timeout_config_selector() const { return _timeout_config_selector; }

    void inc_cql_stats(bool is_internal) const;

    void analyze_condition(expr::expression cond);

    void set_if_not_exist_condition();

    bool has_if_not_exist_condition() const;

    void set_if_exist_condition();

    bool has_if_exist_condition() const;

    /// Decides whether an IF EXISTS / IF NOT EXISTS condition is about the static
    /// row or about a clustering row.  Must run before the checks that read
    /// applies_only_to_static_columns(), which this can change.
    void classify_exists_condition(bool restricts_clustering_columns);

    // True if the statement has IF conditions. Pre-computed during prepare.
    bool has_conditions() const { return _has_regular_column_conditions || _has_static_column_conditions; }
    // True if the statement has IF conditions that apply to static columns.
    bool has_static_column_conditions() const { return _has_static_column_conditions; }
    // True if this statement needs to read only static column values to check if it can be applied.
    bool has_only_static_column_conditions() const { return !_has_regular_column_conditions && _has_static_column_conditions; }

    bool has_regular_column_conditions() const { return _has_regular_column_conditions; }

    /**
     * Checks whether the conditions represented by this statement apply provided the current state of the row on
     * which those conditions are.
     *
     * @param row the row with current data corresponding to these conditions. Can be null if there
     * is no matching row.
     * @return whether the conditions represented by this statement apply or not.
     */
    bool applies_to(const selection::selection* selection, const update_parameters::prefetch_data::row* row, const query_options& options) const;

protected:
    /**
     * If there are conditions on the statement, this is called after the where clause and conditions have been
     * processed to check that they are compatible.  A conditional statement cannot
     * use IN on a key column: it addresses one row.
     * @throws InvalidRequestException
     */
    void reject_in_relations_with_conditions(bool key_is_in_relation, bool clustering_key_has_IN) const;

    friend class raw::modification_statement;
};

}

}
