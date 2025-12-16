/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */
#pragma once

#include <string_view>

struct sstring_hash {
    using is_transparent = void;
    size_t operator()(std::string_view v) const noexcept;
};

struct sstring_eq {
    using is_transparent = void;
    bool operator()(std::string_view a, std::string_view b) const noexcept;
};
