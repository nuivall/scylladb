/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_statement.hh"

namespace cql3::statements::eventual_consistency {

/*
 * A batch committed through storage_proxy: its modifications become one
 * mutation per partition, written with the replication factor's eventual
 * consistency, or a single Paxos round when any of them carries IF conditions.
 */
class batch_statement final : public cql3::statements::batch_statement {
public:
    batch_statement(int bound_terms, type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    batch_statement(type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);
};

}
