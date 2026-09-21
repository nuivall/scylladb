/*
 * Modified by ScyllaDB
 * Copyright (C) 2015-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */
#pragma once

#include "cql3/cql_statement.hh"
#include "raw/batch_statement.hh"
#include "mutation/timestamp.hh"
#include "utils/log.hh"
#include "service_permit.hh"
#include "exceptions/coordinator_result.hh"
#include "tracing/trace_state.hh"

namespace cql_transport::messages {
    class result_message;
}

namespace db {
enum class large_data_violation_type : uint8_t;
}

namespace cql3 {

class query_processor;

namespace statements {

class modification_spec;

/*
 * A <code>BATCH</code> statement parsed from a CQL query.
 *
 * Holds the modifications the batch applies together, and answers everything a
 * cql_statement is asked about them. A sub-class adds only execution: how the
 * mutations they produce are committed.
 */
class batch_statement : public cql_statement {
public:
    using type = raw::batch_statement::type;

    // One modification of the batch, and whether the client still has to be
    // authorized for it - a prepared statement it has already been authorized
    // for does not need checking again.
    struct single_statement {
        shared_ptr<modification_spec> spec;
        bool needs_authorization = true;

    public:
        single_statement(shared_ptr<modification_spec> s)
            : spec(std::move(s))
        {}
        single_statement(shared_ptr<modification_spec> s, bool na)
            : spec(std::move(s))
            , needs_authorization(na)
        {}
    };
protected:
    int _bound_terms;
    type _type;
    std::vector<single_statement> _statements;
    std::unique_ptr<attributes> _attrs;
    // True if *any* statement of the batch has IF .. clause. In
    // this case entire batch is considered a CAS batch.
    bool _has_conditions;
    // If the BATCH has conditions, it must return columns which
    // are involved in condition expressions in its result set.
    // Unlike Cassandra, Scylla always returns all columns,
    // regardless of whether the batch succeeds or not - this
    // allows clients to prepare a CAS statement like any other
    // statement, and trust the returned statement metadata.
    // Cassandra returns a result set only if CAS succeeds. If
    // any statement in the batch has IF EXISTS, we must return
    // all columns of the table, including the primary key.
    column_set _columns_of_cas_result_set;
    cql_stats& _stats;
public:
    /**
     * Creates a new BatchStatement from a list of statements
     *
     * @param type type of the batch
     * @param statements a list of UpdateStatements
     * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
     */
    batch_statement(int bound_terms, type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    batch_statement(type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    virtual bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    // A control connection never has a legitimate reason to run a batch, so any
    // batch arriving on one means it is being used for user load.
    bool should_reclassify_control_connection() const override {
        return true;
    }

    virtual uint32_t get_bound_terms() const override;

    virtual future<> check_access(query_processor& qp, const service::client_state& state) const override;

    // Validates a prepared batch statement without validating its nested statements.
    void validate();

    bool has_conditions() const { return _has_conditions; }

    void build_cas_result_set_metadata();

    // The batch itself will be validated in either Parsed#prepare() - for regular CQL3 batches,
    //   or in QueryProcessor.processBatch() - for native protocol batches.
    virtual void validate(query_processor& qp, const service::client_state& state) const override;

    const std::vector<single_statement>& get_statements() const;

    db::timeout_clock::duration get_timeout(const service::client_state& state, const query_options& options) const;

public:
    // FIXME: no cql_statement::to_string() yet
#if 0
    sstring to_string() const {
        return format("BatchStatement(type={}, statements={})", _type, join(", ", _statements));
    }
#endif
};

}
}
