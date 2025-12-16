/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <string_view>

struct str_hash {
    using is_transparent = void;
    size_t operator()(std::string_view v) const noexcept;
};

struct str_eq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept;
};
