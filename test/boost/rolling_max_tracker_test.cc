/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#define BOOST_TEST_MODULE rolling_max_tracker

#include <boost/test/unit_test.hpp>
#include <limits>

#include "utils/rolling_max_tracker.hh"

BOOST_AUTO_TEST_CASE(test_empty_tracker_returns_zero) {
    utils::rolling_max_tracker tracker;
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 0u);
}

BOOST_AUTO_TEST_CASE(test_single_sample) {
    utils::rolling_max_tracker tracker(10);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);
}

BOOST_AUTO_TEST_CASE(test_max_tracks_largest_in_window) {
    utils::rolling_max_tracker tracker(10);
    tracker.add_sample(5);
    tracker.add_sample(20);
    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 20u);
}

BOOST_AUTO_TEST_CASE(test_increasing_samples) {
    utils::rolling_max_tracker tracker(5);
    for (size_t i = 1; i <= 10; ++i) {
        tracker.add_sample(i);
        BOOST_REQUIRE_EQUAL(tracker.current_max(), i);
    }
}

BOOST_AUTO_TEST_CASE(test_decreasing_samples) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(100);
    tracker.add_sample(90);
    tracker.add_sample(80);
    tracker.add_sample(70);
    tracker.add_sample(60);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);

    tracker.add_sample(50);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 90u);

    tracker.add_sample(40);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 80u);
}

BOOST_AUTO_TEST_CASE(test_max_expires_from_window) {
    utils::rolling_max_tracker tracker(3);
    tracker.add_sample(100);
    tracker.add_sample(1);
    tracker.add_sample(2);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);

    tracker.add_sample(3);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 3u);
}

BOOST_AUTO_TEST_CASE(test_new_max_replaces_smaller_entries) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(10);
    tracker.add_sample(5);
    tracker.add_sample(3);
    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 10u);

    tracker.add_sample(50);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 50u);

    tracker.add_sample(1);
    tracker.add_sample(1);
    tracker.add_sample(1);
    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 50u);

    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 1u);
}

BOOST_AUTO_TEST_CASE(test_window_size_one) {
    utils::rolling_max_tracker tracker(1);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);

    tracker.add_sample(5);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(200);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 200u);
}

BOOST_AUTO_TEST_CASE(test_window_size_two) {
    utils::rolling_max_tracker tracker(2);
    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);

    tracker.add_sample(5);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);

    tracker.add_sample(200);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 200u);

    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 200u);

    tracker.add_sample(10);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 10u);
}

BOOST_AUTO_TEST_CASE(test_equal_values) {
    utils::rolling_max_tracker tracker(5);
    tracker.add_sample(42);
    tracker.add_sample(42);
    tracker.add_sample(42);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 42u);

    for (int i = 0; i < 20; ++i) {
        tracker.add_sample(42);
        BOOST_REQUIRE_EQUAL(tracker.current_max(), 42u);
    }

    tracker.add_sample(100);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 100u);
}

BOOST_AUTO_TEST_CASE(test_staircase_pattern) {
    utils::rolling_max_tracker tracker(6);

    for (size_t i = 1; i <= 5; ++i) {
        tracker.add_sample(i);
    }
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(4);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(3);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(2);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(1);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 5u);

    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 4u);
}

BOOST_AUTO_TEST_CASE(test_large_value) {
    utils::rolling_max_tracker tracker(4);
    auto max = std::numeric_limits<size_t>::max();
    tracker.add_sample(max);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), max);

    tracker.add_sample(0);
    tracker.add_sample(0);
    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), max);

    tracker.add_sample(0);
    BOOST_REQUIRE_EQUAL(tracker.current_max(), 0u);
}

BOOST_AUTO_TEST_CASE(test_sliding_window_correctness) {
    const size_t window = 7;
    const size_t n = 100;
    utils::rolling_max_tracker tracker(window);
    std::vector<size_t> values;
    values.reserve(n);

    for (size_t i = 0; i < n; ++i) {
        values.push_back((i * 37 + 13) % 50);
    }

    for (size_t i = 0; i < n; ++i) {
        tracker.add_sample(values[i]);

        size_t start = (i + 1 > window) ? (i + 1 - window) : 0;
        size_t expected_max = 0;
        for (size_t j = start; j <= i; ++j) {
            if (values[j] > expected_max) {
                expected_max = values[j];
            }
        }
        BOOST_REQUIRE_EQUAL(tracker.current_max(), expected_max);
    }
}
