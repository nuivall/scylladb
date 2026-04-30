/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "data_dictionary/data_dictionary.hh"
#include "cql3/prepare_context.hh"
#include "cql3/column_specification.hh"

#include <seastar/core/shared_ptr.hh>

#include <vector>
#include "audit/audit.hh"

namespace cql3 {

class column_identifier;
class cql_stats;
class cql_config;

namespace statements {

class prepared_statement;

namespace raw {

class parsed_statement {
protected:
    prepare_context _prepare_ctx;

public:
    virtual ~parsed_statement();

    prepare_context& get_prepare_context();
    const prepare_context& get_prepare_context() const;

    void set_bound_variables(const std::vector<::shared_ptr<column_identifier>>& bound_names);

    virtual std::unique_ptr<prepared_statement> prepare(data_dictionary::database db, cql_stats& stats, const cql_config& cfg) = 0;

    // Returns true iff this statement does not depend on the connection's
    // current USE keyspace to resolve table names. The default returns true
    // for statements that don't reference a table at all (e.g. role/permission
    // statements). Statements that may operate on a table override this and
    // report whether the user wrote a fully qualified name.
    //
    // IMPORTANT: must be called BEFORE cf_statement::prepare_keyspace() runs,
    // because that method fills in the keyspace from the connection state and
    // would otherwise hide the original parse-time qualification.
    //
    // Used by query_processor to make the prepared statement id independent
    // of the connection keyspace for fully qualified queries
    // (SCYLLADB-1224 / CASSANDRA-15252).
    virtual bool is_fully_qualified() const {
        return true;
    }

protected:
    virtual audit::statement_category category() const = 0;
    virtual audit::audit_info_ptr audit_info() const = 0;
};

}

}

}
