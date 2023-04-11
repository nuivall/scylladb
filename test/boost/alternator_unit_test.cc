/*
 * Copyright (C) 2020-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#define BOOST_TEST_MODULE alternator
#include <boost/test/included/unit_test.hpp>
#include <boost/test/data/test_case.hpp>

#include <seastar/util/defer.hh>
#include <seastar/core/memory.hh>
#include <vector>

#include "alternator/expressions_parser.hh"
#include "alternator/expressions.hh"
#include "utils/base64.hh"
#include "utils/rjson.hh"

namespace bdata = boost::unit_test::data;

static bytes_view to_bytes_view(const std::string& s) {
    return bytes_view(reinterpret_cast<const signed char*>(s.c_str()), s.size());
}

static std::map<std::string, std::string> strings {
    {"", ""},
    {"a", "YQ=="},
    {"ab", "YWI="},
    {"abc", "YWJj"},
    {"abcd", "YWJjZA=="},
    {"abcde", "YWJjZGU="},
    {"abcdef", "YWJjZGVm"},
    {"abcdefg", "YWJjZGVmZw=="},
    {"abcdefgh", "YWJjZGVmZ2g="},
};

BOOST_AUTO_TEST_CASE(test_base64_encode_decode) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(base64_encode(to_bytes_view(str)), encoded);
        auto decoded = base64_decode(encoded);
        BOOST_REQUIRE_EQUAL(to_bytes_view(str), bytes_view(decoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_decoded_len) {
    for (auto& [str, encoded] : strings) {
        BOOST_REQUIRE_EQUAL(str.size(), base64_decoded_len(encoded));
    }
}

BOOST_AUTO_TEST_CASE(test_base64_begins_with) {
    for (auto& [str, encoded] : strings) {
        for (size_t i = 0; i < str.size(); ++i) {
            std::string prefix(str.c_str(), i);
            std::string encoded_prefix = base64_encode(to_bytes_view(prefix));
            BOOST_REQUIRE(base64_begins_with(encoded, encoded_prefix));
        }
    }
    std::string str1 = "ABCDEFGHIJKL123456";
    std::string str2 = "ABCDEFGHIJKL1234567";
    std::string str3 = "ABCDEFGHIJKL12345678";
    std::string encoded_str1 = base64_encode(to_bytes_view(str1));
    std::string encoded_str2 = base64_encode(to_bytes_view(str2));
    std::string encoded_str3 = base64_encode(to_bytes_view(str3));
    std::vector<std::string> non_prefixes = {
        "B", "AC", "ABD", "ACD", "ABCE", "ABCEG", "ABCDEFGHIJKLM", "ABCDEFGHIJKL123456789"
    };
    for (auto& non_prefix : non_prefixes) {
        std::string encoded_non_prefix = base64_encode(to_bytes_view(non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str1, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str2, encoded_non_prefix));
        BOOST_REQUIRE(!base64_begins_with(encoded_str3, encoded_non_prefix));
    }
}

BOOST_AUTO_TEST_CASE(test_allocator_fail_gracefully) {
    // Allocation size is set to a ridiculously high value to ensure
    // that it will immediately fail - trying to lazily allocate just
    // a little more than total memory may still succeed.
    static size_t too_large_alloc_size = memory::stats().total_memory() * 1024 * 1024;
    rjson::allocator allocator;
    // Impossible allocation should throw
    BOOST_REQUIRE_THROW(allocator.Malloc(too_large_alloc_size), rjson::error);
    // So should impossible reallocation
    void* memory = allocator.Malloc(1);
    auto release = defer([memory] { rjson::allocator::Free(memory); });
    BOOST_REQUIRE_THROW(allocator.Realloc(memory, 1, too_large_alloc_size), rjson::error);
    // Internal rapidjson stack should also throw
    // and also be destroyed gracefully later
    rapidjson::internal::Stack stack(&allocator, 0);
    BOOST_REQUIRE_THROW(stack.Push<char>(too_large_alloc_size), rjson::error);
}

using p = alternator::parsed::path;
using v = std::vector<p>;
using op = p::operator_t;

BOOST_DATA_TEST_CASE(test_expressions_projections_valid, bdata::make({
    std::make_tuple("x1", v{p("x1")}),
    std::make_tuple("#0placeholder", v{p("#0placeholder")}),
    std::make_tuple("#0placeholder.#1placeholder", v{p("#0placeholder",std::vector{op("#1placeholder")})}),
    std::make_tuple("x1, x2", v{p("x1"), p("x2")}),
    std::make_tuple("y[0]", v{p("y", std::vector{op(0u)})}),
    std::make_tuple("y[0][2]", v{p("y", std::vector{op(0u), op(2u)})}),
    std::make_tuple("y.zzz.h", v{p("y", std::vector{op("zzz"), op("h")})}),
    // complex example:
    std::make_tuple("y.zz, gge, x,y,x, h[0].a.b.c.d[123123].eee,   h123_AX", v{
        p("y", std::vector{op("zz")}),
        p("gge"),
        p("x"),
        p("y"),
        p("x"),
        p("h", std::vector{op(0u), op("a"), op("b"), op("c"), op("d"), op(123123u), op("eee")}),
        p("h123_AX"),
    }),
}), input, expected_obj)
{
    auto got_obj = alternator::parse_projection_expression(input);
    BOOST_REQUIRE_EQUAL(got_obj, expected_obj);
}

BOOST_DATA_TEST_CASE(test_expressions_projections_invalid, bdata::make({
    "",
    "x,",
    "1y",
    "#",
    "x#f",
    "[1]",
    ".f",
    "v$@%",
    "x[-1]",
    "g, [0]",
    "g, 123",
    "h[0.xxx]",
    "fun(x)",
}), input)
{
    BOOST_REQUIRE_THROW(alternator::parse_projection_expression(input),
        alternator::expressions_syntax_error);
}

using a = alternator::parsed::action;
using u = alternator::parsed::update_expression;
u make_u(std::vector<a> as) {
    auto ret = u();
    for (auto& ac : as) {
        ret.add(std::move(ac));
    }
    return ret;
}

using val = alternator::parsed::value;
using rhs = alternator::parsed::set_rhs;

rhs rhs_valref(std::string name) {
    rhs r;
    val v;
    v.set_valref(name);
    r.set_value(std::move(v));
    return r;
}

rhs rhs_valref_plus_path(std::string name, p path) {
    rhs r;
    val v, v2;
    v.set_valref(name);
    v2.set_path(path);
    r.set_value(std::move(v));
    r.set_plus(std::move(v2));
    return r;
}

rhs rhs_path_minus_func(p path, val fun) {
    rhs r;
    val v;
    v.set_path(path);
    r.set_value(std::move(v));
    r.set_minus(std::move(fun));
    return r;
}

rhs rhs_val(val v) {
    rhs r;
    r.set_value(std::move(v));
    return r;
}

BOOST_DATA_TEST_CASE(test_expressions_update_valid, bdata::make({
    std::make_tuple("REMOVE xyz", make_u(std::vector{a::make_remove(p("xyz"))})),
    std::make_tuple("REMOVE #y", make_u(std::vector{a::make_remove(p("#y"))})),
    std::make_tuple("ReMoVe x.y[1]", make_u(std::vector{a::make_remove(p("x", std::vector{op("y"), op(1u)}))})),
    std::make_tuple("REMOVE a,bb,  ccc", make_u(std::vector{
        a::make_remove(p("a")), 
        a::make_remove(p("bb")), 
        a::make_remove(p("ccc"))})),
    std::make_tuple("ADD v1 :val, v[2] :val2", make_u(std::vector{
        a::make_add(p("v1"), ":val"), 
        a::make_add(p("v", std::vector{op(2u)}),":val2")})),
    std::make_tuple("DELETE x.y :val", make_u(std::vector{
        a::make_del(p("x", std::vector{op("y")}),":val")})),
    std::make_tuple("SET path = :val, path2=:val2", make_u(std::vector{
        a::make_set(p("path"), rhs_valref(":val")),
        a::make_set(p("path2"), rhs_valref(":val2"))})),
    std::make_tuple("SET path = :val + path2", make_u(std::vector{
        a::make_set(p("path"), rhs_valref_plus_path(":val", p("path2")))})),
    std::make_tuple("SET path = path2 - fun(#nameref, :valref, path)", make_u(std::vector{
        a::make_set(p("path"), rhs_path_minus_func(p("path2"), []() {
            val v, v2, v3, v4;
            v.set_func_name("fun");
            v2.set_path(p("#nameref"));
            v3.set_valref(":valref");
            v4.set_path(p("path"));
            v.add_func_parameter(v2);
            v.add_func_parameter(v3);
            v.add_func_parameter(v4);
            return v;
        }()))
    })),
    std::make_tuple("SET path = funA(path,funB(funC(:valref),   funD(funE(:valref2)) ) ), #pathref = funF( #pathref)", make_u(std::vector{
        a::make_set(p("path"), rhs_val([]() {
            val fA, fB, fC, fD, fE;
            fA.set_func_name("funA");
            fB.set_func_name("funB");
            fC.set_func_name("funC");
            fD.set_func_name("funD");
            fE.set_func_name("funE");

            val valref, valref2, path;
            valref.set_valref(":valref");
            valref2.set_valref(":valref2");
            path.set_path(p("path"));

            fC.add_func_parameter(valref);
            fE.add_func_parameter(valref2);

            fD.add_func_parameter(fE);
            fB.add_func_parameter(fC);
            fB.add_func_parameter(fD);

            fA.add_func_parameter(path);
            fA.add_func_parameter(fB);
            return fA;
        }())),
        a::make_set(p("#pathref"), rhs_val([]() {
            val fF;
            fF.set_func_name("funF");
            val pathref;
            pathref.set_path(p("#pathref"));
            fF.add_func_parameter(pathref);
            return fF;
        }()))
    })),
}), input, expected_obj)
{
    auto got_obj = alternator::parse_update_expression(input);
    BOOST_REQUIRE_EQUAL(got_obj, expected_obj);
}

BOOST_DATA_TEST_CASE(test_expressions_update_invalid, bdata::make({
    "",
    "REMOVE  ",
    "REMOVE a REMOVE b",
    "ADD  ",
    // Too much nesting.
    "f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(f(#ref))))))))))))))))))))))))))))))))))))))",
    // 12 is not valref.
    "ADD Fefe 12",
    "DELETE Fefe 12",
    "ADD fefe, ",
    "SET :valref = :valref2",
    "SET path = fun(",
    "SET path = fun()",
    "SET path = fun())",
    "SET path = :val,",
    "SET path = :valfun()",
}), input)
{
    BOOST_REQUIRE_THROW(alternator::parse_update_expression(input),
        alternator::expressions_syntax_error);
}
