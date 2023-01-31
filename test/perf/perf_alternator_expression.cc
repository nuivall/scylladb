/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/testing/perf_tests.hh>
#include "alternator/expressions.hh"

struct exp_test {
};

PERF_TEST_F(exp_test, condition_simple) {
    perf_tests::do_not_optimize(
        // models given partition key and sort key
        alternator::parse_condition_expression("p = :pv AND q = :qv")
    );
}

PERF_TEST_F(exp_test, condition_and_in) {
    perf_tests::do_not_optimize(
        alternator::parse_condition_expression("p = :pv AND q in (:a, :b, fefe)")
    );
}

PERF_TEST_F(exp_test, perf_in_query_exp_2) {
    perf_tests::do_not_optimize(
        alternator::parse_condition_expression("p = :pv AND q in (:a, :b, :c, :d, :e, :f, :g, :h, :i, :j, :k, :l, :m, :n, :o, :p, :q, :r, :s, :t, :u, :v, :w, :x, :y, :z)")
    );
}