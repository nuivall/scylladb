/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 *
 * Copyright (C) 2021-present ScyllaDB
 *
 */

#pragma once

#include "seastarx.hh"
#include "utils/shared_memory_pool.hh"
#include "service/qos/qos_configuration_change_subscriber.hh"

#include <seastar/core/semaphore.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/metrics_registration.hh>
#include <unordered_map>
#include <functional>
#include <fmt/format.h>

namespace qos {
class service_level_controller;
}

namespace service {

/// Exception thrown when a request exceeds the service level's memory capacity.
/// This differs from the transport-level "request size too large" error, which
/// applies to the global limit. This exception indicates that the request is
/// too large even with the shared pool for the specific service level.
class request_too_large_for_service_level : public std::runtime_error {
public:
    request_too_large_for_service_level(size_t request_size, size_t max_capacity)
        : std::runtime_error(fmt::format(
            "request memory estimate ({} bytes) exceeds service level capacity ({} bytes)",
            request_size, max_capacity)) {}
};

/// RAII wrapper for semaphore units that tracks memory borrowed from a shared pool.
/// When the units are released (object destroyed), any borrowed memory is returned
/// to the shared pool, and the corresponding inflation of the semaphore is undone.
class memory_units {
    semaphore_units<> _sem_units;
    shared_memory_pool* _pool;
    semaphore* _sem;
    ssize_t _borrowed;
public:
    memory_units() noexcept : _pool(nullptr), _sem(nullptr), _borrowed(0) {}

    memory_units(semaphore_units<>&& units, shared_memory_pool* pool, semaphore* sem, ssize_t borrowed) noexcept
        : _sem_units(std::move(units))
        , _pool(pool)
        , _sem(sem)
        , _borrowed(borrowed) {}

    memory_units(memory_units&& o) noexcept
        : _sem_units(std::move(o._sem_units))
        , _pool(o._pool)
        , _sem(o._sem)
        , _borrowed(o._borrowed) {
        o._pool = nullptr;
        o._sem = nullptr;
        o._borrowed = 0;
    }

    memory_units& operator=(memory_units&& o) noexcept {
        if (this != &o) {
            release();
            _sem_units = std::move(o._sem_units);
            _pool = o._pool;
            _sem = o._sem;
            _borrowed = o._borrowed;
            o._pool = nullptr;
            o._sem = nullptr;
            o._borrowed = 0;
        }
        return *this;
    }

    ~memory_units() {
        release();
    }

    memory_units(const memory_units&) = delete;
    memory_units& operator=(const memory_units&) = delete;

    size_t count() const noexcept { return _sem_units.count(); }

    void return_units(size_t to_return) {
        if (to_return == 0) {
            return;
        }
        // Return all units to the semaphore first
        _sem_units.return_units(to_return);
        // Then, for any borrowed portion being returned, undo the semaphore
        // inflation and return memory to the shared pool instead.
        if (_borrowed > 0) {
            ssize_t from_borrowed = std::min(static_cast<ssize_t>(to_return), _borrowed);
            // Consume from semaphore to undo the inflation done during get_units
            (void)_sem->consume(from_borrowed);
            _pool->signal(from_borrowed);
            _borrowed -= from_borrowed;
        }
    }

private:
    void release() noexcept {
        // Before the semaphore_units destructor returns borrowed memory to the
        // semaphore, consume the borrowed portion to undo the inflation, and
        // return it to the shared pool.
        if (_borrowed > 0 && _pool && _sem) {
            // The _sem_units destructor will signal(count) to the semaphore.
            // We need to undo the borrowed portion of that signal.
            // Consume borrowed from the semaphore (may make it negative temporarily,
            // but _sem_units destructor will signal immediately after).
            (void)_sem->consume(_borrowed);
            _pool->signal(_borrowed);
            _borrowed = 0;
        }
    }
};

/// Per-service-level transport memory limiter.
///
/// Reserves 5% of total shard memory for in-flight request payloads at the
/// transport layer (CQL and Alternator). The budget is divided among service
/// levels proportional to their shares:
///   - A dedicated portion (50% by default) is split among per-SL semaphores
///   - A shared pool (50% by default) can be borrowed by any SL when its
///     dedicated budget is exhausted
///
/// Implements qos::qos_configuration_change_subscriber to react to service
/// level add/remove/change events.
class memory_limiter final : public qos::qos_configuration_change_subscriber {
public:
    static constexpr size_t default_shared_pool_pct = 50;
    static constexpr size_t memory_fraction = 20; // 5% = 1/20

private:
    struct weighted_semaphore {
        size_t weight;
        size_t dedicated_memory;
        semaphore sem;
        seastar::metrics::metric_groups metrics;

        weighted_semaphore(size_t w, size_t mem)
            : weight(w)
            , dedicated_memory(mem)
            , sem(mem) {}

        // Not copyable/movable due to semaphore
        weighted_semaphore(const weighted_semaphore&) = delete;
        weighted_semaphore& operator=(const weighted_semaphore&) = delete;
    };

    size_t _mem_total;
    uint32_t _shared_pool_pct;
    shared_memory_pool _shared_pool;
    std::unordered_map<scheduling_group, std::unique_ptr<weighted_semaphore>> _semaphores;
    scheduling_group _default_sg;
    size_t _total_weight = 0;

    std::function<future<>()> _unsubscribe_qos;

    void adjust_semaphores();
    void add_semaphore(scheduling_group sg, size_t shares);
    weighted_semaphore& get_or_default(scheduling_group sg);

public:
    /// Construct with total available memory. Allocates 5% (1/20) as the
    /// transport memory budget. Per-service-level semaphores are not set up
    /// until start() is called.
    explicit memory_limiter(size_t available_memory, uint32_t shared_pool_pct = default_shared_pool_pct) noexcept;

    future<> stop();

    size_t total_memory() const noexcept { return _mem_total; }

    /// Initialize per-service-level semaphores. Must be called after the
    /// service_level_controller is started and before any transport server
    /// begins accepting requests. Registers as a QoS subscriber and sets
    /// up per-SL semaphores for all existing service levels.
    future<> start(qos::service_level_controller& sl_controller);

    /// Get memory units from the per-SL semaphore for the current scheduling group.
    /// Falls back to the default SL semaphore if no matching SL is found.
    /// Throws request_too_large_for_service_level if amount exceeds SL capacity.
    future<memory_units> get_units(size_t amount);

    /// Get memory units with a timeout (for load shedding).
    future<memory_units> get_units(size_t amount, lowres_clock::duration timeout);

    /// Get memory units for an explicit scheduling group.
    future<memory_units> get_units(scheduling_group sg, size_t amount);

    /// Check if there are waiters on the semaphore for the given scheduling group.
    bool has_waiters() const;
    bool has_waiters(scheduling_group sg) const;

    /// Get aggregate available memory across all per-SL semaphores + shared pool.
    size_t available_memory() const;

    // qos::qos_configuration_change_subscriber
    future<> on_before_service_level_add(qos::service_level_options slo, qos::service_level_info sl_info) override;
    future<> on_after_service_level_remove(qos::service_level_info sl_info) override;
    future<> on_before_service_level_change(qos::service_level_options slo_before,
        qos::service_level_options slo_after, qos::service_level_info sl_info) override;
    future<> on_effective_service_levels_cache_reloaded() override;
};

} // namespace service
