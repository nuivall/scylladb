/*
 * Copyright (C) 2017-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "auth/cache.hh"
#include "auth/roles-metadata.hh"
#include "cql3/query_processor.hh"
#include "cql3/untyped_result_set.hh"
#include "db/consistency_level_type.hh"
#include "db/system_keyspace.hh"
#include "schema/schema.hh"
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/core/format.hh>

namespace auth {

cache::cache(cql3::query_processor& qp) noexcept
    : _current_version(0)
    , _qp(&qp) {
}

seastar::lw_shared_ptr<cache::role_record> cache::get(const role_name_t& role) const noexcept {
    auto it = _roles.find(role);
    if (it == _roles.end()) {
        return {};
    }
    return it->second;
}

seastar::future<seastar::lw_shared_ptr<cache::role_record>> cache::load_role(const role_name_t& role) {
    auto rec = seastar::make_lw_shared<role_record>();
    rec->version = _current_version;

    auto fetch_single = [this](const sstring& q) {
        return _qp->execute_internal(q, db::consistency_level::LOCAL_ONE,
                internal_distributed_query_state(), {},
                cql3::query_processor::cache_internal::no);
    };
    // roles
    {
        static const sstring q = seastar::format("SELECT * FROM {}.{} WHERE role='{}' LIMIT 1", db::system_keyspace::NAME, meta::roles_table::name, role);
        auto rs = co_await fetch_single(q);
        if (!rs->empty()) {
            auto& r = rs->one();
            rec->is_superuser = r.get_as<bool>("is_superuser");
            rec->can_login = r.get_as<bool>("can_login");
            auto mo = r.get_set<sstring>("member_of");
            rec->member_of.insert(mo.begin(), mo.end());
            rec->salted_hash = r.get_as<sstring>("salted_hash");
        }
    }
    // members
    {
        static const sstring q = seastar::format("SELECT role, member FROM {}.{} WHERE role='{}' LIMIT 1", db::system_keyspace::NAME, ROLE_MEMBERS_CF, role);
        auto rs = co_await fetch_single(q);
        if (!rs->empty()) {
            auto& r = rs->one();
            rec->members.insert(r.get_as<sstring>("member"));
        }
    }
    // attributes
    {
        static const sstring q = seastar::format("SELECT role, name, value FROM {}.{} WHERE role='{}' LIMIT 1", db::system_keyspace::NAME, ROLE_ATTRIBUTES_CF, role);
        auto rs = co_await fetch_single(q);
        if (!rs->empty()) {
            auto& r = rs->one();
            auto attr_name = r.get_as<sstring>("name");
            auto value = r.get_as<sstring>("value");
            rec->attributes.emplace(attr_name, value);
        }
    }
    // permissions
    {
        static const sstring q = seastar::format("SELECT {}, {}, {} FROM {}.{} WHERE role='{}' LIMIT 1", ROLE_NAME, RESOURCE_NAME, PERMISSIONS_NAME, db::system_keyspace::NAME, PERMISSIONS_CF, role);
        auto rs = co_await fetch_single(q);
        if (!rs->empty()) {
            auto& r = rs->one();
            auto resource = r.get_as<sstring>("resource");
            auto perms_strings = r.get_set<sstring>("permissions");
            std::unordered_set<sstring> perms_set(perms_strings.begin(), perms_strings.end());
            auto pset = permissions::from_strings(perms_set);
            rec->permissions[resource] = pset;
        }
    }
    co_return rec;
}

seastar::future<> cache::prune_all() noexcept {
    for (auto it = _roles.begin(); it != _roles.end();) {
        if (it->second->version != _current_version) {
            _roles.erase(it++);
            co_await seastar::maybe_yield();
        } else {
            ++it;
        }
    }
    co_return;
}

seastar::future<> cache::load_all() {
    SCYLLA_ASSERT(this_shard_id() == 0);
    ++_current_version;

    const uint32_t page_size = 128;
    auto loader = [this](const cql3::untyped_result_set::row& r) -> seastar::future<stop_iteration> {
        auto role = r.get_as<sstring>("role");
        auto rec = co_await load_role(role);
        _roles[role] = rec;
        co_return stop_iteration::no;
    };
    co_await _qp->query_internal(seastar::format("SELECT * FROM {}.{}", db::system_keyspace::NAME, meta::roles_table::name),
            db::consistency_level::LOCAL_ONE, {}, page_size, loader);

    co_await prune_all();
    co_await container().invoke_on_others([ver = _current_version, roles_snapshot = _roles](cache& c) -> seastar::future<> {
        c._current_version = ver;
        for (auto& role : roles_snapshot) {
            auto& name = role.first;
            // deep copy record per shard
            auto rec = seastar::make_lw_shared<cache::role_record>(*role.second);
            c._roles[name] = std::move(rec);
            co_await seastar::maybe_yield();
        }
        co_await c.prune_all();
    });
}

seastar::future<> cache::load_roles(std::unordered_set<role_name_t>  roles) {
    for (auto& name : roles) {
        auto rec = co_await load_role(name);
        _roles[name] = rec;
        co_await seastar::maybe_yield();
    }
    co_await container().invoke_on_others([this, &roles](cache& c) -> seastar::future<> {
        for (auto& name : roles) {
            // deep copy record per shard
            auto rec = seastar::make_lw_shared<cache::role_record>(*get(name));
            c._roles[name] = std::move(rec);
            co_await seastar::maybe_yield();
        }
    });
}

bool cache::includes_table(const table_id& id) noexcept {
    return id == db::system_keyspace::roles()->id()
            || id == db::system_keyspace::role_members()->id()
            || id == db::system_keyspace::role_attributes()->id()
            || id == db::system_keyspace::role_permissions()->id();
}

} // namespace auth
