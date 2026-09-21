/*
 * Copyright (C) 2015-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (LicenseRef-ScyllaDB-Source-Available-1.1 and Apache-2.0)
 */

#pragma once

#include "cql3/cql_statement.hh"
#include "cql3/statements/modification_spec.hh"

#include <seastar/core/shared_ptr.hh>

namespace cql3 {

namespace statements {

/*
 * A single modification - an INSERT, an UPDATE or a DELETE - as the CQL server
 * executes it.
 *
 * The statement holds the modification_spec that parsing produced and adds
 * nothing to it but execution: a sub-class commits the spec's mutation through
 * storage_proxy or through Raft. Everything else a cql_statement is asked -
 * access control, validation, the bound terms - the spec answers.
 */
class modification_statement : public cql_statement {
    const ::shared_ptr<modification_spec> _spec;

public:
    explicit modification_statement(::shared_ptr<modification_spec> spec);

    virtual ~modification_statement() override;

    // What this statement executes. Borrowed, so only valid while it lives.
    const modification_spec& spec() const { return *_spec; }

    // The same, for a caller which has to keep the spec alive on its own, e.g.
    // a batch collecting the modifications it commits together.
    const ::shared_ptr<modification_spec>& shared_spec() const { return _spec; }

    uint32_t get_bound_terms() const override;

    future<> check_access(query_processor& qp, const service::client_state& state) const override;

    void validate(query_processor& qp, const service::client_state& state) const override;

    bool depends_on(std::string_view ks_name, std::optional<std::string_view> cf_name) const override;

    bool should_reclassify_control_connection() const override;

    bool is_conditional() const override;

    seastar::shared_ptr<const metadata> get_result_metadata() const override;
};
}

}
