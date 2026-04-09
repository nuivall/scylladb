/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 *
 * Copyright (C) 2026-present ScyllaDB
 *
 */

#include "service/memory_limiter.hh"
#include "service/qos/service_level_controller.hh"

#include <seastar/core/scheduling.hh>
#include <seastar/core/metrics.hh>
#include <cmath>

namespace service {

static seastar::logger mllog("memory_limiter");

memory_limiter::memory_limiter(size_t available_memory, uint32_t shared_pool_pct) noexcept
    : _mem_total(available_memory / memory_fraction)
    , _shared_pool_pct(shared_pool_pct)
    , _shared_pool(0)
{
}

future<> memory_limiter::stop() {
    if (_unsubscribe_qos) {
        co_await _unsubscribe_qos();
    }
}

future<> memory_limiter::start(qos::service_level_controller& sl_controller) {
    sl_controller.register_subscriber(this);
    _unsubscribe_qos = [this, &sl_controller] () {
        return sl_controller.unregister_subscriber(this);
    };

    qos::service_level default_sl = sl_controller.get_service_level(qos::service_level_controller::default_service_level_name);
    int32_t default_shares = 1000;
    if (auto* shares_p = std::get_if<int32_t>(&default_sl.slo.shares)) {
        default_shares = *shares_p;
    }

    _default_sg = default_sl.sg;
    add_semaphore(default_sl.sg, default_shares);

    auto service_levels = co_await sl_controller.get_distributed_service_levels(qos::query_context::group0);
    for (auto&& [name, slo] : service_levels) {
        auto sl = sl_controller.get_service_level(name);
        if (sl.slo.shares_name && *sl.slo.shares_name != qos::service_level_controller::default_service_level_name) {
            add_semaphore(sl.sg, std::get<int32_t>(sl.slo.shares));
        }
    }

    adjust_semaphores();

    mllog.info("Transport memory limiter initialized: total={}B, shared_pool={}%, per-SL semaphores={}",
        _mem_total, _shared_pool_pct, _semaphores.size());

    co_return;
}

void memory_limiter::add_semaphore(scheduling_group sg, size_t shares) {
    auto it = _semaphores.find(sg);
    if (it != _semaphores.end()) {
        // Update existing
        auto old_weight = it->second->weight;
        _total_weight -= old_weight;
        it->second->weight = shares;
        _total_weight += shares;
    } else {
        // Create new
        auto ws = std::make_unique<weighted_semaphore>(shares, 0);
        _semaphores.emplace(sg, std::move(ws));
        _total_weight += shares;
    }
}

void memory_limiter::adjust_semaphores() {
    if (_total_weight == 0 || _semaphores.empty()) {
        return;
    }

    const ssize_t shared_memory = (_mem_total * _shared_pool_pct) / 100;
    const ssize_t dedicated_memory = _mem_total - shared_memory;
    _shared_pool.set_total_memory(shared_memory);

    ssize_t distributed_memory = 0;
    for (auto& [sg, wsem] : _semaphores) {
        const ssize_t memory_share = static_cast<ssize_t>(
            std::floor(static_cast<double>(wsem->weight) / static_cast<double>(_total_weight) * dedicated_memory));

        // Adjust the semaphore: compute delta from current dedicated memory
        ssize_t delta = memory_share - static_cast<ssize_t>(wsem->dedicated_memory);
        if (delta > 0) {
            wsem->sem.signal(delta);
        } else if (delta < 0) {
            // Try to consume. If current() goes negative, requests will
            // block until enough memory is freed.
            (void)wsem->sem.consume(-delta);
        }
        wsem->dedicated_memory = memory_share;
        distributed_memory += memory_share;
    }

    // Slap the remainder on one of the semaphores (a few bytes from floor rounding)
    if (dedicated_memory > distributed_memory) {
        auto& first = _semaphores.begin()->second;
        ssize_t remainder = dedicated_memory - distributed_memory;
        first->sem.signal(remainder);
        first->dedicated_memory += remainder;
    }
}

memory_limiter::weighted_semaphore& memory_limiter::get_or_default(scheduling_group sg) {
    auto it = _semaphores.find(sg);
    if (it != _semaphores.end()) {
        return *it->second;
    }
    // Fall back to the default SL semaphore
    auto def_it = _semaphores.find(_default_sg);
    assert(def_it != _semaphores.end());
    return *def_it->second;
}

future<memory_units> memory_limiter::get_units(size_t amount) {
    return get_units(current_scheduling_group(), amount);
}

future<memory_units> memory_limiter::get_units(size_t amount, lowres_clock::duration timeout) {
    auto& wsem = get_or_default(current_scheduling_group());
    size_t max_capacity = wsem.dedicated_memory + _shared_pool.total_memory();
    if (amount > max_capacity) {
        throw request_too_large_for_service_level(amount, max_capacity);
    }

    // Try to borrow from the shared pool to avoid blocking on the semaphore.
    // We borrow the shortfall between what's available and what's needed.
    ssize_t to_borrow = 0;
    size_t avail = wsem.sem.current();
    if (avail < amount) {
        ssize_t shortfall = static_cast<ssize_t>(amount - avail);
        if (_shared_pool.try_consume(shortfall)) {
            wsem.sem.signal(shortfall);
            to_borrow = shortfall;
        }
        // If try_consume fails, we'll just block on the semaphore
        // until other requests free up dedicated memory.
    }

    try {
        auto units = co_await seastar::get_units(wsem.sem, amount, timeout);
        co_return memory_units(std::move(units), &_shared_pool, &wsem.sem, to_borrow);
    } catch (...) {
        // On failure, return any borrowed memory
        if (to_borrow > 0) {
            // We signaled the semaphore with borrowed memory but get_units
            // failed (e.g., timeout). Consume back the borrowed portion
            // and return it to the shared pool.
            (void)wsem.sem.consume(to_borrow);
            _shared_pool.signal(to_borrow);
        }
        throw;
    }
}

future<memory_units> memory_limiter::get_units(scheduling_group sg, size_t amount) {
    auto& wsem = get_or_default(sg);
    size_t max_capacity = wsem.dedicated_memory + _shared_pool.total_memory();
    if (amount > max_capacity) {
        throw request_too_large_for_service_level(amount, max_capacity);
    }

    // Try to borrow from shared pool if dedicated memory is insufficient
    ssize_t to_borrow = 0;
    size_t avail = wsem.sem.current();
    if (avail < amount) {
        ssize_t shortfall = static_cast<ssize_t>(amount - avail);
        if (_shared_pool.try_consume(shortfall)) {
            wsem.sem.signal(shortfall);
            to_borrow = shortfall;
        }
    }

    auto units = co_await seastar::get_units(wsem.sem, amount);
    co_return memory_units(std::move(units), &_shared_pool, &wsem.sem, to_borrow);
}

bool memory_limiter::has_waiters() const {
    return has_waiters(current_scheduling_group());
}

bool memory_limiter::has_waiters(scheduling_group sg) const {
    auto it = _semaphores.find(sg);
    if (it != _semaphores.end()) {
        return it->second->sem.waiters() > 0;
    }
    auto def_it = _semaphores.find(_default_sg);
    if (def_it != _semaphores.end()) {
        return def_it->second->sem.waiters() > 0;
    }
    return false;
}

size_t memory_limiter::available_memory() const {
    size_t total = _shared_pool.available_memory();
    for (auto& [sg, wsem] : _semaphores) {
        total += std::max<ssize_t>(0, wsem->sem.current());
    }
    return total;
}

// qos::qos_configuration_change_subscriber implementation

future<> memory_limiter::on_before_service_level_add(qos::service_level_options slo, qos::service_level_info sl_info) {
    if (auto shares_p = std::get_if<int32_t>(&slo.shares)) {
        add_semaphore(sl_info.sg, *shares_p);
        adjust_semaphores();
        mllog.debug("Added transport memory semaphore for SL '{}' with shares={}", sl_info.name, *shares_p);
    }
    co_return;
}

future<> memory_limiter::on_after_service_level_remove(qos::service_level_info sl_info) {
    auto it = _semaphores.find(sl_info.sg);
    if (it != _semaphores.end()) {
        _total_weight -= it->second->weight;
        _semaphores.erase(it);
        adjust_semaphores();
        mllog.debug("Removed transport memory semaphore for SL '{}'", sl_info.name);
    }
    co_return;
}

future<> memory_limiter::on_before_service_level_change(
        qos::service_level_options slo_before,
        qos::service_level_options slo_after,
        qos::service_level_info sl_info) {
    if (auto shares_p = std::get_if<int32_t>(&slo_after.shares)) {
        add_semaphore(sl_info.sg, *shares_p);
        adjust_semaphores();
        mllog.debug("Updated transport memory semaphore for SL '{}' with shares={}", sl_info.name, *shares_p);
    }
    co_return;
}

future<> memory_limiter::on_effective_service_levels_cache_reloaded() {
    return make_ready_future<>();
}

} // namespace service
