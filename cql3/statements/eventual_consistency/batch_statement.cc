/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include "cql3/statements/eventual_consistency/batch_statement.hh"

#include "cql3/attributes.hh"

namespace cql3::statements::eventual_consistency {

batch_statement::batch_statement(int bound_terms, type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : cql3::statements::batch_statement(bound_terms, type_, std::move(statements), std::move(attrs), stats)
{
    validate();
    if (has_conditions()) {
        // A batch can be created not only by raw::batch_statement::prepare, but also by
        // cql_server::connection::process_batch, which doesn't call any methods of
        // cql3::statements::batch_statement, only constructs it. So let's call
        // build_cas_result_set_metadata right from the constructor to avoid crash trying to access
        // uninitialized batch metadata.
        build_cas_result_set_metadata();
    }
}

batch_statement::batch_statement(type type_,
                                 std::vector<single_statement> statements,
                                 std::unique_ptr<attributes> attrs,
                                 cql_stats& stats)
    : batch_statement(-1, type_, std::move(statements), std::move(attrs), stats)
{
}

}
