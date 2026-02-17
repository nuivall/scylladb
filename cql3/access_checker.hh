/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/core/checked_ptr.hh>

namespace auth {
class service;
}

namespace cql3 {

class query_processor;
class authorized_prepared_statements_cache_key;

namespace statements {
class prepared_statement;
}

/// \brief Gate-protected class that re-runs check_access during cache reloads.
///
/// Owned by auth::service and plugged into the authorized_prepared_statements_cache.
/// The seastar::gate prevents use-after-stop of auth::service when the reload callback fires.
class access_checker {
    query_processor& _qp;
    auth::service& _auth_service;
    seastar::gate _gate;

public:
    using key_type = authorized_prepared_statements_cache_key;
    using value_type = seastar::checked_ptr<seastar::weak_ptr<const statements::prepared_statement>>;

    access_checker(query_processor& qp, auth::service& auth_service)
        : _qp(qp)
        , _auth_service(auth_service) {
    }

    /// Re-run check_access for a cached entry.
    /// Returns a valid checked_weak_ptr if authorization succeeds, or an empty one if it fails.
    seastar::future<value_type> check(const key_type& k, value_type existing);

    /// Close the gate and wait for in-flight check_access calls to complete.
    seastar::future<> stop();
};

} // namespace cql3
