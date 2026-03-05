/*
 * Copyright (C) 2019-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <seastar/core/semaphore.hh>
#include <seastar/core/shared_ptr.hh>

class service_permit {
    seastar::lw_shared_ptr<seastar::semaphore_units<>> _permit;
    seastar::semaphore* _sem = nullptr;
    service_permit(seastar::semaphore_units<>&& u) : _permit(seastar::make_lw_shared<seastar::semaphore_units<>>(std::move(u))) {}
    service_permit(seastar::semaphore_units<>&& u, seastar::semaphore& sem)
        : _permit(seastar::make_lw_shared<seastar::semaphore_units<>>(std::move(u)))
        , _sem(&sem) {}
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit);
    friend service_permit make_service_permit(seastar::semaphore_units<>&& permit, seastar::semaphore& sem);
    friend service_permit empty_service_permit();
public:
    size_t count() const { return _permit ? _permit->count() : 0; };

    /// Consume additional units from the semaphore (non-blocking) and adopt
    /// them into this permit. Used to adjust when actual resource usage
    /// exceeds the initially estimated amount.
    void adopt_extra_units(size_t n) {
        if (_sem && _permit && n > 0) {
            _permit->adopt(seastar::consume_units(*_sem, n));
        }
    }
};

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit) {
    return service_permit(std::move(permit));
}

inline service_permit make_service_permit(seastar::semaphore_units<>&& permit, seastar::semaphore& sem) {
    return service_permit(std::move(permit), sem);
}

inline service_permit empty_service_permit() {
    return make_service_permit(seastar::semaphore_units<>());
}
