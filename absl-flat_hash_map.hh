/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <absl/container/flat_hash_map.h>
#include <seastar/core/sstring.hh>

#include "utils/heterogeneous_lookup.hh"

using namespace seastar;

template <typename K, typename V, typename... Ts>
struct flat_hash_map : public absl::flat_hash_map<K, V, Ts...> {
};

template <typename V>
struct flat_hash_map<sstring, V>
    : public absl::flat_hash_map<sstring, V, sstring_hash, sstring_eq> {};
