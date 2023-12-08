/*
 * Modified by ScyllaDB
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: (AGPL-3.0-or-later and Apache-2.0)
 */

#pragma once

#include "db/config.hh"
#include "schema/schema_fwd.hh"
#include <vector>

namespace db {

namespace system_auth_keyspace {
    static constexpr auto NAME = "system_auth_v2";
    // tables
    static constexpr auto ROLES = "roles";

    std::vector<schema_ptr> all_tables(const db::config& cfg);
}; // namespace system_auth_keyspace

} // namespace db
