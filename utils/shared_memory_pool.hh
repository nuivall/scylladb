/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstddef>
#include <unistd.h>

/// A shared pool of memory that can be used by multiple semaphores.
/// When a semaphore exhausts its dedicated memory, it can borrow from this pool.
class shared_memory_pool {
    ssize_t _available_memory;
    size_t _total_memory;
public:
    explicit shared_memory_pool(size_t memory = 0) noexcept
        : _available_memory(memory)
        , _total_memory(memory) {}

    /// Try to consume `amount` bytes from the pool.
    /// Returns true if the pool had enough memory and the amount was consumed.
    /// Returns false if the pool does not have enough memory (nothing is consumed).
    bool try_consume(size_t amount) noexcept {
        if (_available_memory >= 0 && static_cast<size_t>(_available_memory) >= amount) {
            _available_memory -= amount;
            return true;
        }
        return false;
    }

    /// Return `amount` bytes back to the pool.
    void signal(size_t amount) noexcept {
        _available_memory += amount;
    }

    ssize_t available_memory() const noexcept {
        return _available_memory;
    }

    size_t total_memory() const noexcept {
        return _total_memory;
    }

    /// Adjust the total memory of the pool. The available memory is adjusted
    /// by the same delta, so it may go negative if memory is being borrowed.
    void set_total_memory(size_t memory) noexcept {
        ssize_t diff = static_cast<ssize_t>(memory) - static_cast<ssize_t>(_total_memory);
        _total_memory = memory;
        _available_memory += diff;
    }
};
