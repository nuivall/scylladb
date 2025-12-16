/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include "utils/heterogeneous_lookup.hh"

#include <absl/hash/hash.h>
#include <seastar/core/sstring.hh>

size_t str_hash::operator()(std::string_view v) const noexcept {
    return absl::Hash<std::string_view>{}(v);
}

bool str_eq::operator()(std::string_view a, std::string_view b) const noexcept {
    return a == b;
}

