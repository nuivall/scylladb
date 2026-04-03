/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include "service/memory_limiter.hh"
#include <seastar/core/shared_ptr.hh>

class service_permit {
    seastar::lw_shared_ptr<service::memory_units> _permit;
    service_permit(service::memory_units&& u) : _permit(seastar::make_lw_shared<service::memory_units>(std::move(u))) {}
    friend service_permit make_service_permit(service::memory_units&& permit);
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit);
    friend service_permit empty_service_permit();
public:
    size_t count() const { return _permit ? _permit->count() : 0; };

    void return_units(size_t to_return) {
        if (_permit) {
            _permit->return_units(to_return);
        }
    }
};

inline service_permit make_service_permit(service::memory_units&& permit) {
    return service_permit(std::move(permit));
}

/// Deprecated overload for callers not yet migrated to memory_units.
inline service_permit make_service_permit(seastar::semaphore_units<>&& permit) {
    return service_permit(service::memory_units(std::move(permit), nullptr, nullptr, 0));
}

inline service_permit empty_service_permit() {
    return make_service_permit(service::memory_units());
}
