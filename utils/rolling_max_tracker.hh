/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <utility>

namespace utils {

/// Tracks the rolling maximum over the last `window_size` samples
/// using a monotonic deque, providing amortized O(1) add_sample()
/// and O(1) current_max().
///
/// Invariant: the deque stores (sequence_number, value) pairs with
/// strictly decreasing values. When a new sample arrives, all entries
/// from the back with value <= the new sample are removed (they can
/// never be the maximum for any future window position). Entries
/// that have fallen out of the window are removed from the front.
class rolling_max_tracker {
    std::deque<std::pair<uint64_t, size_t>> _deque;
    uint64_t _seq = 0;
    size_t _window_size;

public:
    explicit rolling_max_tracker(size_t window_size = 1000) noexcept
        : _window_size(window_size) {
    }

    void add_sample(size_t value) noexcept {
        // Maintain the monotonic (decreasing) property:
        // remove all entries from the back that are <= the new value,
        // since they can never be the maximum while this entry is in the window.
        while (!_deque.empty() && _deque.back().second <= value) {
            _deque.pop_back();
        }
        _deque.emplace_back(_seq, value);
        ++_seq;
        if (_seq == 0) [[unlikely]] {
            _deque.clear();
            return;
        }
        // Remove entries that have fallen out of the window from the front.
        while (_deque.front().first + _window_size < _seq) {
            _deque.pop_front();
        }
    }

    size_t current_max() const noexcept {
        return _deque.empty() ? 0 : _deque.front().second;
    }
};

} // namespace utils
