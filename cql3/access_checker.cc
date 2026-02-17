/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "cql3/access_checker.hh"
#include "cql3/authorized_prepared_statements_cache.hh"
#include "cql3/cql_statement.hh"
#include "cql3/query_processor.hh"
#include "cql3/statements/prepared_statement.hh"
#include "service/client_state.hh"
#include "auth/authenticated_user.hh"

#include <seastar/core/coroutine.hh>

namespace cql3 {

seastar::future<access_checker::value_type> access_checker::check(const key_type& k, value_type existing) {
    auto holder = _gate.try_hold();
    if (!holder) {
        // Gate is closing — auth::service is stopping. Signal eviction.
        co_return value_type();
    }

    const statements::prepared_statement* ps;
    try {
        ps = &*existing;
    } catch (seastar::checked_ptr_is_null_exception&) {
        // Prepared statement was invalidated.
        co_return value_type();
    }

    auto stmt = ps->statement;

    // Construct a lightweight auth_context for the re-check.
    // The user comes from the cache key; _auth_service comes from this access_checker.
    service::auth_context ctx(k.key().first, _auth_service);

    try {
        co_await stmt->check_access(_qp, ctx);
        // Still authorized — keep in cache.
        co_return std::move(existing);
    } catch (...) {
        // Authorization failed — signal eviction.
        co_return value_type();
    }
}

seastar::future<> access_checker::stop() {
    return _gate.close();
}

} // namespace cql3
