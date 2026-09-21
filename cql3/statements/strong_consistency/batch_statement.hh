/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#pragma once

#include "cql3/statements/batch_statement.hh"

namespace cql3::statements::strong_consistency {

/*
 * A batch committed through the Raft group which owns the partition its
 * modifications address: their mutations are merged into one, which is what
 * makes the batch atomic, and is also why they all have to target the same
 * partition.
 */
class batch_statement final : public cql3::statements::batch_statement {
    using result_message = cql_transport::messages::result_message;

public:
    batch_statement(int bound_terms, type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    batch_statement(type type_,
                    std::vector<single_statement> statements,
                    std::unique_ptr<attributes> attrs,
                    cql_stats& stats);

    future<shared_ptr<result_message>> execute(query_processor& qp, service::query_state& state,
        const query_options& options, std::optional<service::group0_guard> guard) const override;

    future<shared_ptr<result_message>> execute_without_checking_exception_message(query_processor& qp,
        service::query_state& qs, const query_options& options,
        std::optional<service::group0_guard> guard) const override;

private:
    // Rejects what the strongly consistent write path cannot honour. Runs from
    // the constructor, like the eventually consistent batch's own validation.
    void validate_strongly_consistent() const;
};

}
