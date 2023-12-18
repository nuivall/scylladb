/*
 * Copyright (C) 2016-present ScyllaDB
 *
 * Modified by ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include <functional>

#include <seastar/core/abort_source.hh>

#include "auth/authorizer.hh"
#include "service/migration_manager.hh"

namespace cql3 {

class query_processor;

} // namespace cql3

namespace auth {

class default_authorizer;

// This class contains pre auth-v2 feature code
// where data resides in system_auth as opposed
// to system_auth_v2 in current implementation.
class default_authorizer_legacy_impl {
    cql3::query_processor& _qp;
    ::service::migration_manager& _migration_manager;
    abort_source _as{};
    future<> _finished{make_ready_future<>()};

    friend default_authorizer;
    default_authorizer& _main_impl;

    default_authorizer_legacy_impl(default_authorizer& main_impl, cql3::query_processor& qp, ::service::migration_manager& mm)
        : _qp(qp)
        , _migration_manager(mm)
        , _main_impl(main_impl) {
    }

    future<> start();
    future<> stop();
    future<> migrate_legacy_metadata() const;
    bool legacy_metadata_exists() const;
};

class default_authorizer : public authorizer {
    cql3::query_processor& _qp;
    std::string_view _auth_ks_name;

    friend default_authorizer_legacy_impl;
    default_authorizer_legacy_impl _legacy_impl;

public:
    default_authorizer(cql3::query_processor&, ::service::migration_manager&);

    ~default_authorizer();

    virtual future<> start() override;

    virtual future<> stop() override;

    virtual std::string_view qualified_java_name() const override;

    virtual future<permission_set> authorize(const role_or_anonymous&, const resource&) const override;

    virtual future<> grant(std::string_view, permission_set, const resource&) const override;

    virtual future<> revoke( std::string_view, permission_set, const resource&) const override;

    virtual future<std::vector<permission_details>> list_all() const override;

    virtual future<> revoke_all(std::string_view) const override;

    virtual future<> revoke_all(const resource&) const override;

    virtual const resource_set& protected_resources() const override;

private:
    future<bool> any_granted() const;

    future<> modify(std::string_view, permission_set, const resource&, std::string_view) const;
};

} /* namespace auth */

