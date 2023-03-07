/*
 * Copyright (C) 2023-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: AGPL-3.0-or-later
 */

#include <seastar/core/thread.hh>
#include <seastar/core/app-template.hh>

namespace perf {

using namespace seastar;

std::function<int(int, char**)> alternator_workloads(std::function<int(int, char**)> scylla_main) {
    return [=](int argc, char** argv) -> int {
        app_template app;
        return app.run(argc, argv, [scylla_main, argc, argv] {
             // start scylla in the background
            return seastar::async([=] {
                (void)scylla_main(argc, argv);
                 std::cout << "testing..." << std::endl;
            });
        });
    };
}

} // namespace perf



    // app_template app;
    // app.add_options()
    //         ("https", bpo::bool_switch(), "Use HTTPS on port 443 (if off -- use HTTP on port 80)")
    //         ("host", bpo::value<std::string>(), "Host to connect")
    //         ("path", bpo::value<std::string>(), "Path to query upon")
    //         ("method", bpo::value<std::string>()->default_value("GET"), "Method to use")
    //         ("file", bpo::value<std::string>(), "File to get body from (no body if missing)")
    // ;


    // return app.run(ac, av, [&] {
    //     auto&& config = app.configuration();
    //     auto host = config["host"].as<std::string>();
    //     auto path = config["path"].as<std::string>();
    //     auto method = config["method"].as<std::string>();
    //     auto body = config.count("file") == 0 ? std::string("") : config["file"].as<std::string>();
    //     auto https = config["https"].as<bool>();

    //     return seastar::async([=] {
    //         net::hostent e = net::dns::get_host_by_name(host, net::inet_address::family::INET).get0();
    //         auto make_socket = [=] () -> connected_socket {
    //             if (!https) {
    //                 socket_address local = socket_address(::sockaddr_in{AF_INET, INADDR_ANY, {0}});
    //                 ipv4_addr addr(e.addr_list.front(), 80);
    //                 fmt::print("{} {}:80{}\n", method, e.addr_list.front(), path);
    //                 return connect(make_ipv4_address(addr), local, transport::TCP).get0();
    //             } else {
    //                 auto certs = ::make_shared<tls::certificate_credentials>();
    //                 certs->set_system_trust().get();
    //                 socket_address remote = socket_address(e.addr_list.front(), 443);
    //                 fmt::print("{} {}:443{}\n", method, e.addr_list.front(), path);
    //                 return tls::connect(certs, remote, host).get0();
    //             }
    //         };

    //         connected_socket s = make_socket();
    //         http::experimental::connection conn(std::move(s));
    //         auto req = http::request::make(method, host, path);
    //         if (body != "") {
    //             future<file> f = open_file_dma(body, open_flags::ro);
    //             req.write_body("txt", [ f = std::move(f) ] (output_stream<char>&& out) mutable {
    //                 return seastar::async([f = std::move(f), out = std::move(out)] () mutable {
    //                     auto in = make_file_input_stream(f.get0());
    //                     copy(in, out).get();
    //                     out.flush().get();
    //                     out.close().get();
    //                     in.close().get();
    //                 });
    //             });
    //         }
    //         http::reply rep = conn.make_request(std::move(req)).get0();

    //         fmt::print("Reply status {}\n--------8<--------\n", rep._status);
    //         auto in = conn.in(rep);
    //         in.consume(printer{}).get();
    //         in.close().get();

    //         conn.close().get();
    //     }).handle_exception([](auto ep) {
    //         fmt::print("Error: {}", ep);
    //     });
    // });