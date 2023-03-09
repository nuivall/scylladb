/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <functional>
#include <memory>
#include <signal.h>
#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/sleep.hh>
#include <seastar/http/client.hh>
#include <seastar/http/request.hh>
#include <seastar/http/reply.hh>
#include <seastar/util/defer.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/short_streams.hh>
#include <tuple>
#include <boost/program_options.hpp>

#include "db/config.hh"
#include "seastar/core/future.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"


namespace perf {

using namespace seastar;
using namespace std::chrono_literals;
namespace bpo = boost::program_options;

struct test_config {
    std::string workload;
    int port;
    unsigned partitions;
    unsigned duration_in_seconds;
    unsigned concurrency;
    bool flush;
};

std::ostream& operator<<(std::ostream& os, const test_config& cfg) {
    return os << "{workload=" << cfg.workload
           << ", partitions=" << cfg.partitions
           << ", concurrency=" << cfg.concurrency
           << ", duration_in_seconds=" << cfg.duration_in_seconds
           << ", flush=" << cfg.flush
           << "}";
}

static http::experimental::client get_client(int port) {
    return http::experimental::client(socket_address(net::inet_address("127.0.0.1"), port));
}

static future<> make_request(http::experimental::client& cli, sstring operation, sstring body) {
    auto req = http::request::make("POST", "localhost", "/");
    req._headers["X-Amz-Target:"] = "DynamoDB_20120810." + operation;
    req.write_body("application/x-amz-json-1.0", std::move(body));
    return cli.make_request(std::move(req), [] (const http::reply& rep, input_stream<char>&& in) {
        return seastar::async([in = std::move(in)] () mutable {
            util::skip_entire_stream(in).get();
            in.close().get();
        });
    });
}

static void delete_alternator_table(http::experimental::client& cli) {
    try {
        make_request(cli, "DeleteTable", R"({"TableName": "workloads_test"})").get();
    } catch(...) {
        // table may exist or not
    }
}

static void create_alternator_table(http::experimental::client& cli) {
    delete_alternator_table(cli); // cleanup in case of leftovers
    make_request(cli, "CreateTable", R"(
        {
            "AttributeDefinitions": [{
                    "AttributeName": "p",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "c",
                    "AttributeType": "S"
                }
            ],
            "TableName": "workloads_test",
            "BillingMode": "PAY_PER_REQUEST",
            "KeySchema": [{
                    "AttributeName": "p",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "c",
                    "KeyType": "RANGE"
                }
            ]
        }
    )").get();
}

static future<> update_item(http::experimental::client& cli, uint64_t seq) {
    // Exercise various types documented here: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html 
    auto prefix = format(R"({{
            "TableName": "workloads_test",
            "Key": {{
                "p": {{
                    "S": "{}"
                }},
                "c": {{
                    "S": "{}"
                }}
            }},)", seq, seq);
    auto suffix = R"(
            "UpdateExpression": "set C0 = :C0, C1 = :C1, C2 = :C2, C3 = :C3, C4 = :C4, C5 = :C5, C6 = :C6, C7 = :C7, C8 = :C8, C9 = :C9",
            "ExpressionAttributeValues": {
                ":C0": {
                    "B": "dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk"
                },
                ":C1": {
                   "BOOL": true
                },
                ":C2": {
                    "BS": ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]
                },
                ":C3": {
                    "L": [ {"S": "Cookies"} , {"S": "Coffee"}, {"N": "3.14159"}]
                },
                ":C4": {
                    "M": {"Name": {"S": "Joe"}, "Age": {"N": "35"}}
                },
                ":C5": {
                    "N": "123.45"
                },
                ":C6": {
                    "NS": ["42.2", "-19", "7.5", "3.14"]
                },
                ":C7": {
                    "NULL": true
                },
                ":C8": {
                    "S": "Hello"
                },
                ":C9": {
                    "SS": ["Giraffe", "Hippo" ,"Zebra"]
                }
            },
            "ReturnValues": "NONE"
        }
    )";
    return make_request(cli, "UpdateItem", prefix+suffix);
}

static future<> get_item(http::experimental::client& cli, uint64_t seq) {
    auto body = format(R"({{
        "TableName": "workloads_test",
        "Key": {{
            "p": {{
                "S": "{}"
            }},
            "c": {{
                "S": "{}"
            }}
        }},
        "ProjectionExpression": "C0, C1, C2, C3, C4, C5, C6, C7, C8, C9",
        "ConsistentRead": false,
        "ReturnConsumedCapacity": "TOTAL"
    }})",seq, seq);
    return make_request(cli, "GetItem", std::move(body));
}

static void flush_table() {
    auto cli = get_client(10000);
    auto req = http::request::make("POST", "localhost", "/storage_service/keyspace_flush/alternator_workloads_test");
    cli.make_request(std::move(req), [] (const http::reply& rep, input_stream<char>&& in) {
        return make_ready_future<>();
    }).get();
    cli.close().get();
}

static void create_partitions(const test_config& c, http::experimental::client& cli) {
    std::cout << "Creating " << c.partitions << " partitions..." << std::endl;
    for (unsigned seq = 0; seq < c.partitions; ++seq) {
        update_item(cli, seq).get();
    }
    if (c.flush) {
        std::cout << "Flushing partitions..." << std::endl;
        flush_table();
    }
}

void workload_main(const test_config& c) {
    std::cout << "Running test with config: " << c << std::endl;

    auto cli = get_client(c.port);
    auto finally = defer([&] {
        delete_alternator_table(cli);
        cli.close().get();
    });

    create_alternator_table(cli);
    using fun_t = std::function<future<>(http::experimental::client&, uint64_t)>;
    std::map<std::string, fun_t> workloads = {
        {"read",  get_item},
        {"write", update_item},
    };

    if (c.workload == "read") {
        create_partitions(c, cli);
    }

    auto it = workloads.find(c.workload);
    if (it == workloads.end()) {
        throw std::runtime_error(format("unknown workload '{}'", c.workload));
    }
    fun_t fun = it->second;

    auto results = time_parallel([&] {
        static thread_local auto sharded_cli = get_client(c.port); // for simplicity never closed as it lives for the whole process runtime
        auto seq = tests::random::get_int<uint64_t>(c.partitions - 1);
        return fun(sharded_cli, seq);
    }, c.concurrency, c.duration_in_seconds);

    std::cout << aggregated_perf_results(results) << std::endl;
}

std::tuple<int,char**> cut_arg(int ac, char** av, std::string name, int num_args = 2) {
    for (int i = 1 ; i < ac - 1; i++) {
        if (std::string(av[i]) == name) {
            std::shift_left(av + i, av + ac, num_args);
            ac -= num_args;
            break;
        }
    }
    return std::make_tuple(ac, av);
}

std::function<int(int, char**)> alternator_workloads(std::function<int(int, char**)> scylla_main, std::function<void(lw_shared_ptr<db::config> cfg)>* after_init_func) {
    return [=](int ac, char** av) -> int {
        test_config c;
       
        bpo::options_description opts_desc;
        opts_desc.add_options()
            ("workload", bpo::value<std::string>()->default_value(""), "which workload type to run")
            ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
            ("duration", bpo::value<unsigned>()->default_value(5), "test duration in seconds")
            ("concurrency", bpo::value<unsigned>()->default_value(100), "workers per core")
            ("flush", bpo::value<bool>()->default_value(true), "flush memtables before test")
        ;
        bpo::variables_map opts;
        bpo::store(bpo::command_line_parser(ac, av).options(opts_desc).allow_unregistered().run(), opts);

        c.workload = opts["workload"].as<std::string>();
        c.partitions = opts["partitions"].as<unsigned>();
        c.duration_in_seconds = opts["duration"].as<unsigned>();
        c.concurrency = opts["concurrency"].as<unsigned>();
        c.flush = opts["flush"].as<bool>();

        // Remove test options to not disturb scylla main app
        for (auto& opt : opts_desc.options()) {
            auto name = opt->canonical_display_name(bpo::command_line_style::allow_long);
            std::tie(ac, av) = cut_arg(ac, av, name);
        }

        if (c.workload.empty()) {
            std::cerr << "Missing --workload command-line value!" << std::endl;
            return 1;
        }
        
        *after_init_func = [c = std::move(c)] (lw_shared_ptr<db::config> cfg) mutable {
            c.port = cfg->alternator_port();
            (void)seastar::async([c = std::move(c)] {
                try {
                    workload_main(c);
                } catch(...) {
                    std::cerr << "Test failed: " << std::current_exception() << std::endl;
                    raise(SIGKILL); // request abnormal shutdown
                }
                raise(SIGINT); // request shutdown
            });
        };
        return scylla_main(ac, av);
    };
}

} // namespace perf