/*
 * Copyright (C) 2025-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <cstdlib>
#include <limits>
#include <memory>
#include <seastar/core/app-template.hh>
#include <seastar/core/abort_source.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/signal.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/byteorder.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/lowres_clock.hh>
#include <fmt/format.h>
#include <seastar/util/log.hh>
#include <signal.h>

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include "db/config.hh"
#include "test/perf/perf.hh"
#include "test/lib/random_utils.hh"
#include "transport/server.hh"
#include "transport/response.hh"
#include <cstring>
#include <unordered_map>
#include <stack>

namespace perf {
using namespace seastar;
namespace bpo = boost::program_options;
using namespace cql_transport;

// Small hand and AI crafted CQL client that  builds raw
// frames directly and sends over a tcp connection to exercise the full
// CQL binary protocol parsing path without any external driver layers.

struct raw_cql_test_config {
    std::string workload; // read | write | connect
    unsigned partitions;  // number of partitions existing / to write
    unsigned duration_in_seconds;
    unsigned operations_per_shard;
    unsigned concurrency_per_connection; // requests per connection
    unsigned connections_per_shard; // connections per shard
    bool continue_after_error;
    uint16_t port = 9042; // native transport port
    std::string username = ""; // optional auth username
    std::string password = ""; // optional auth password
    std::string remote_host = ""; // target host for CQL + REST (empty => in-process server mode)
    bool connection_per_request = false; // create and tear down a connection for every request
    bool create_non_superuser = false;
    std::string replication = "simple"; // "simple" => SimpleStrategy RF=1, "nts" => NTS AWS_US_WEST_2:3
    std::string json_result_file;
};

} // namespace perf

template <>
struct fmt::formatter<perf::raw_cql_test_config> {
    constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
    auto format(const perf::raw_cql_test_config& c, format_context& ctx) const {
        return fmt::format_to(ctx.out(), "{{workload={}, partitions={}, concurrency={}, connections={}, replication={}, duration={}, ops_per_shard={}{}{}}}",
            c.workload, c.partitions, c.concurrency_per_connection, c.connections_per_shard, c.replication, c.duration_in_seconds, c.operations_per_shard,
            (c.username.empty() ? "" : ", auth"),
            (c.connection_per_request ? ", connection_per_request" : ""),
            (c.create_non_superuser ? ", create_non_superuser" : ""));
    }
};

namespace perf {

// Basic frame building helpers (CQL v4)
// Binary protocol v4 header is 9 bytes:
//  0: version (request direction bit clear, thus 0x04)
//  1: flags
//  2: stream id (msb)
//  3: stream id (lsb)
//  4: opcode
//  5..8: body length (big endian)
struct frame_builder {
    static constexpr size_t header_size = 9;
    static constexpr size_t initial_capacity = 1024;

    int16_t stream_id;
    temporary_buffer<char> body;
    size_t pos = header_size;

    frame_builder(int16_t stream) : stream_id(stream), body(initial_capacity) {}

    void ensure_space(size_t n) {
        size_t cap = body.size();
        if (pos + n <= cap) {
            return;
        }
        size_t new_cap = std::max(cap * 2, pos + n);
        temporary_buffer<char> nb(new_cap);
        std::memcpy(nb.get_write(), body.get(), pos);
        body = std::move(nb);
    }

    void write_int(int32_t v) {
        ensure_space(4);
        write_be<int32_t>(body.get_write() + pos, v);
        pos += 4;
    }
    void write_short(uint16_t v) {
        ensure_space(2);
        write_be<uint16_t>(body.get_write() + pos, v);
        pos += 2;
    }
    void write_byte(char c) {
        ensure_space(1);
        body.get_write()[pos++] = c;
    }
    void write_raw(const char* data, size_t len) {
        ensure_space(len);
        std::memcpy(body.get_write() + pos, data, len);
        pos += len;
    }
    void write_string(std::string_view s) {
        write_short(s.size());
        write_raw(s.data(), s.size());
    }
    void write_long_string(std::string_view s) {
        write_int(s.size());
        write_raw(s.data(), s.size());
    }
    void write_bytes(std::string_view s) {
        write_int(s.size());
        write_raw(s.data(), s.size());
    }
    temporary_buffer<char> finish(cql_binary_opcode op) {
        size_t body_len = pos - header_size;
        auto* p = body.get_write();
        p[0] = 0x04;
        p[1] = 0;
        write_be<int16_t>(p + 2, stream_id);
        p[4] = static_cast<uint8_t>(op);
        write_be<int32_t>(p + 5, body_len);
        body.trim(pos);
        return std::move(body);
    }
};

// Benchmark data model constants.
//
// Schema: benchks.data (pk text, ck int, payload text, tags map<text,text>)
// Each partition has `num_ck_rows` clustering rows (ck 0..6).
// payload is ~100KB of deterministic text; tags is a map with ~50 entries of
// ~1KB each (~50KB total). Together a row is ~150KB, and a full partition
// read (~7 rows) returns ~1MB.
//
// Reads use a large IN clause (100 values 0..99) to stress the CQL3 parser;
// only 7 rows actually exist per partition.

static constexpr unsigned num_ck_rows = 7;       // clustering rows per partition
static constexpr unsigned in_clause_size = 100;   // IN list width for reads
static constexpr size_t payload_size = 100'000;   // ~100KB text payload per row
static constexpr size_t map_entries = 50;         // entries in the tags map
static constexpr size_t map_value_size = 1'000;   // ~1KB per map value

// Partition key: "p-<seq>"
static sstring pk_for(uint64_t seq) {
    return fmt::format("p-{}", seq);
}

// Generate deterministic payload text of `payload_size` bytes.
static sstring make_payload(uint64_t seq, unsigned ck) {
    // Build a seed string and repeat it to fill the target size.
    auto seed = fmt::format("payload-{}-{}-", seq, ck);
    sstring result(sstring::initialized_later(), payload_size);
    size_t slen = seed.size();
    for (size_t i = 0; i < payload_size; ++i) {
        result[i] = seed[i % slen];
    }
    return result;
}

// Generate a CQL map literal: {'k0':'vvvv...', 'k1':'vvvv...', ...}
// with `map_entries` entries each having a ~`map_value_size` value.
static sstring make_map_literal(uint64_t seq, unsigned ck) {
    // Pre-size: each entry is ~(key ~10 chars + value ~1000 chars + quotes/punctuation ~12)
    // Total ~ map_entries * (map_value_size + 30) + 2 braces
    std::string buf;
    buf.reserve(map_entries * (map_value_size + 40) + 2);
    buf.push_back('{');
    for (size_t i = 0; i < map_entries; ++i) {
        if (i > 0) buf.append(", ");
        // Key
        auto key = fmt::format("k{}", i);
        buf.push_back('\'');
        buf.append(key);
        buf.push_back('\'');
        buf.append(": '");
        // Value: deterministic fill
        auto vseed = fmt::format("v{}-{}-{}-", seq, ck, i);
        size_t vslen = vseed.size();
        for (size_t j = 0; j < map_value_size; ++j) {
            buf.push_back(vseed[j % vslen]);
        }
        buf.push_back('\'');
    }
    buf.push_back('}');
    return sstring(buf.data(), buf.size());
}

// Build the read-query IN list string once: "0,1,2,...,99"
static sstring build_in_list() {
    std::string buf;
    buf.reserve(in_clause_size * 4); // generous
    for (unsigned i = 0; i < in_clause_size; ++i) {
        if (i > 0) buf.push_back(',');
        buf.append(std::to_string(i));
    }
    return sstring(buf.data(), buf.size());
}

static const sstring& in_list_string() {
    static const sstring s = build_in_list();
    return s;
}

class raw_cql_connection {
    connected_socket _cs;
    input_stream<char> _in;
    output_stream<char> _out;
    semaphore _connection_sem{1};
    sstring _username;
    sstring _password;

    struct frame {
        cql_binary_opcode opcode;
        int16_t stream;
        temporary_buffer<char> payload;
    };

    std::unordered_map<int16_t, promise<frame>> _requests;
    future<> _reader_done = make_ready_future<>();
    bool _reader_stopped = false;
    std::stack<int16_t> _free_streams;
    int16_t _next_new_stream = 0;

public:
    raw_cql_connection(connected_socket cs, sstring username = {}, sstring password = {})
        : _cs(std::move(cs)), _in(_cs.input()), _out(_cs.output()), _username(std::move(username)), _password(std::move(password)) {
        start_reader();
    }

    future<> stop() {
        _reader_stopped = true;
        try {
            co_await _in.close();
            co_await _out.close();
        } catch (...) {
            // ignore
        }
        try {
            co_await std::move(_reader_done);
        } catch (...) {
            // ignore
        }
    }

    void start_reader() {
        _reader_done = reader_loop();
    }

    future<> reader_loop() {
        while (!_reader_stopped) {
            try {
                auto f = co_await read_one_frame_internal();
                if (auto it = _requests.find(f.stream); it != _requests.end()) {
                    it->second.set_value(std::move(f));
                    _requests.erase(it);
                }
            } catch (...) {
                auto ep = std::current_exception();
                for (auto& [id, pr] : _requests) {
                    pr.set_exception(ep);
                }
                _requests.clear();
                _reader_stopped = true;
            }
        }
    }

    struct stream_guard {
        raw_cql_connection* _c = nullptr;
        int16_t _id;
        stream_guard(raw_cql_connection* c, int16_t id) : _c(c), _id(id) {}
        stream_guard(stream_guard&& x) noexcept : _c(x._c), _id(x._id) { x._c = nullptr; }
        ~stream_guard() {
            if (_c) {
                _c->release_stream(_id);
             }
        }
        operator int16_t() const { return _id; }
    };

    void release_stream(int16_t stream) {
        _free_streams.push(stream);
    }

    stream_guard allocate_stream() {
        int16_t s;
        if (!_free_streams.empty()) {
            s = _free_streams.top();
            _free_streams.pop();
        } else {
            s = _next_new_stream++;
            if (_next_new_stream == std::numeric_limits<int16_t>::max()) {
                logger l("abort");
                l.error("stream id collision, aborting");
                abort();
            }
        }
        return stream_guard(this, s);
    }

    future<> send_frame(temporary_buffer<char> buf) {
        auto units = co_await get_units(_connection_sem, 1);
        co_await _out.write(std::move(buf));
        co_await _out.flush();
    }

    future<frame> read_one_frame_internal() {
        static constexpr size_t header_size = 9;
        auto hdr_buf = co_await _in.read_exactly(header_size);
        if (hdr_buf.empty()) {
            throw std::runtime_error("connection closed");
        }
        if (hdr_buf.size() != header_size) {
            throw std::runtime_error("short frame header");
        }
        const char* h = hdr_buf.get();
        uint8_t version = h[0];
        (void)version; // unused currently
        uint8_t flags = h[1]; (void)flags;
        uint16_t stream = read_be<uint16_t>(h + 2);
        auto opcode = static_cast<cql_binary_opcode>(h[4]);
        uint32_t len = read_be<uint32_t>(h + 5);

        // Basic protocol sanity checks to catch framing issues early.
        if ((version & 0x7F) != 0x04) {
            throw std::runtime_error(fmt::format("unexpected protocol version byte 0x{:02x} (expected 0x84/0x04)", version));
        }
        if (len > (32u << 20)) { // 32MB arbitrary safety limit
            throw std::runtime_error(fmt::format("suspiciously large frame body length {} > 32MB (malformed?)", len));
        }
        auto body = co_await _in.read_exactly(len);
        if (body.size() != len) {
            throw std::runtime_error("short frame body");
        }
        co_return frame{opcode, stream, std::move(body)};
    }

    future<frame> execute_request(int16_t stream, temporary_buffer<char> buf) {
        promise<frame> p;
        auto f = p.get_future();
        _requests.emplace(stream, std::move(p));
        try {
            co_await send_frame(std::move(buf));
        } catch (...) {
            _requests.erase(stream);
            throw;
        }
        co_return co_await std::move(f);
    }

    future<> startup() {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        // STARTUP frame body (v4): <map<string,string>> of options
        // map encodes with a <short n> for number of entries, then n*(<string><string>)
        fb.write_short(1); // one entry
        fb.write_string("CQL_VERSION");
        fb.write_string("3.0.0");

        auto frame = co_await execute_request(stream, fb.finish(cql_binary_opcode::STARTUP));
        auto op = frame.opcode;
        auto payload = std::move(frame.payload);

        // If user supplied credentials we require the server to challenge with AUTHENTICATE.
        if (!_username.empty() && op != cql_binary_opcode::AUTHENTICATE) {
            throw std::runtime_error("--username specified but server did not request authentication (expected AUTHENTICATE frame)");
        }
        if (op == cql_binary_opcode::AUTHENTICATE) {
            // Assume PasswordAuthenticator; send SASL PLAIN (no need to inspect class name).
            frame_builder auth_fb{stream}; // reuse same stream id per protocol spec
            if (_username.empty()) {
                // Send empty bytes (legacy AllowAll / will trigger error if auth required but no creds supplied)
                auth_fb.write_int(0);
            } else {
                // SASL PLAIN: 0x00 username 0x00 password
                std::string plain;
                plain.reserve(2 + _username.size() + _password.size());
                plain.push_back('\0');
                plain.append(_username.c_str(), _username.size());
                plain.push_back('\0');
                plain.append(_password.c_str(), _password.size());
                auth_fb.write_int(plain.size());
                auth_fb.write_raw(plain.data(), plain.size());
            }
            auto res = co_await execute_request(stream, auth_fb.finish(cql_binary_opcode::AUTH_RESPONSE));
            op = res.opcode;
            payload = std::move(res.payload);
        }
        if (op != cql_binary_opcode::READY && op != cql_binary_opcode::AUTH_SUCCESS) {
            // Try to decode ERROR for better diagnostics
            if (op == cql_binary_opcode::ERROR && payload.size() >= 4) {
                int32_t code = read_be<int32_t>(payload.get());
                // message string follows: <string>
                if (payload.size() >= 6) {
                    auto p = payload.get() + 4;
                    uint16_t slen = read_be<uint16_t>(p);
                    p += 2;
                    sstring msg;
                    if (payload.size() >= 6 + slen) {
                        msg = sstring(p, slen);
                    }
                    throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got ERROR code={} msg='{}'", code, msg));
                }
            }
            throw std::runtime_error(fmt::format("expected READY/AUTH_SUCCESS, got opcode {}", static_cast<int>(op)));
        }
        if (!_username.empty()) {
            // With credentials expect AUTH_SUCCESS explicitly.
            if (op != cql_binary_opcode::AUTH_SUCCESS) {
                throw std::runtime_error("authentication expected AUTH_SUCCESS but got different opcode");
            }
        }
    }

    // CQL binary protocol consistency levels.
    static constexpr uint16_t CL_ONE = 0x0001;
    static constexpr uint16_t CL_ALL = 0x0005;

    future<> query_simple(std::string_view q, uint16_t consistency = CL_ONE) {
        auto stream = allocate_stream();
        frame_builder fb{stream};
        // QUERY frame (v4): <long string><short consistency><byte flags>
        fb.write_long_string(q);
        fb.write_short(consistency);
        fb.write_byte(0); // flags
        auto f = co_await execute_request(stream, fb.finish(cql_binary_opcode::QUERY));
        if (f.opcode == cql_binary_opcode::ERROR) {
            throw std::runtime_error(format("server returned ERROR to QUERY: {}", std::string_view(f.payload.get(), f.payload.size())));
        }
    }

    future<> write_one(uint64_t seq) {
        // Write a single random clustering row for the given partition.
        auto pk = pk_for(seq);
        unsigned ck = tests::random::get_int<unsigned>(num_ck_rows - 1);
        co_await write_one_ck(seq, ck);
    }

    future<> write_one_ck(uint64_t seq, unsigned ck) {
        auto pk = pk_for(seq);
        auto payload = make_payload(seq, ck);
        auto map_lit = make_map_literal(seq, ck);
        auto q = fmt::format("INSERT INTO benchks.data (pk, ck, payload, tags) "
            "VALUES ('{}', {}, '{}', {})",
            pk, ck, payload, map_lit);
        co_await query_simple(q, CL_ALL);
    }

    future<> read_one(uint64_t seq) {
        auto pk = pk_for(seq);
        auto q = fmt::format("SELECT payload, tags FROM benchks.data "
            "WHERE pk = '{}' AND ck IN ({}) "
            "USING TIMEOUT 10000ms",
            pk, in_list_string());
        co_await query_simple(q, CL_ALL);
    }
};

static future<> ensure_schema(raw_cql_connection& conn, const raw_cql_test_config& cfg) {
    if (cfg.replication == "nts") {
        co_await conn.query_simple(
            "CREATE KEYSPACE IF NOT EXISTS benchks WITH replication = "
            "{'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'AWS_US_WEST_2': '3'} "
            "AND durable_writes = true AND tablets = {'enabled': false}");
    } else {
        co_await conn.query_simple(
            "CREATE KEYSPACE IF NOT EXISTS benchks WITH replication = "
            "{'class': 'NetworkTopologyStrategy'} AND tablets = {'enabled': false}");
    }
    co_await conn.query_simple(
        "CREATE TABLE IF NOT EXISTS benchks.data ("
        "  pk text,"
        "  ck int,"
        "  payload text,"
        "  tags map<text, text>,"
        "  PRIMARY KEY (pk, ck)"
        ") WITH CLUSTERING ORDER BY (ck ASC)"
        "  AND bloom_filter_fp_chance = 0.01"
        "  AND caching = {'keys': 'ALL', 'rows_per_partition': 'ALL'}"
        "  AND compaction = {'class': 'IncrementalCompactionStrategy', 'min_threshold': '6', 'space_amplification_goal': '1.25'}"
        "  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}"
        "  AND default_time_to_live = 0"
        "  AND gc_grace_seconds = 864000"
        "  AND speculative_retry = '99.0PERCENTILE'");
}

static future<> create_role_with_permissions(raw_cql_connection& conn, std::string_view username, std::string_view password) {
    co_await conn.query_simple(fmt::format("CREATE ROLE IF NOT EXISTS '{}' WITH PASSWORD = '{}' AND LOGIN = true", username, password));
    co_await conn.query_simple(fmt::format("GRANT SELECT ON benchks.data TO {}", username));
    co_await conn.query_simple(fmt::format("GRANT MODIFY ON benchks.data TO {}", username));
}

static constexpr std::string_view non_superuser_name = "perf_test_user";
static constexpr std::string_view non_superuser_password = "perf_test_password";

static std::unique_ptr<raw_cql_connection> make_connection(connected_socket cs, const raw_cql_test_config& cfg) {
    sstring username = cfg.create_non_superuser ? sstring(non_superuser_name) : sstring(cfg.username);
    sstring password = cfg.create_non_superuser ? sstring(non_superuser_password) : sstring(cfg.password);
    return std::make_unique<raw_cql_connection>(std::move(cs), username, password);
}

// Perform one logical operation (write or read) using an existing connection.
static future<> do_request(raw_cql_connection& c, const raw_cql_test_config& cfg) {
    auto seq = tests::random::get_int<uint64_t>(cfg.partitions - 1);
    if (cfg.workload == "write") {
        co_await c.write_one(seq);
    } else {
        co_await c.read_one(seq);
    }
}

// Create a fresh connection, run a single operation, then let it go out of scope.
static future<> run_one_with_new_connection(const raw_cql_test_config& cfg) {
    connected_socket cs;
    try {
        cs = co_await connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port});
    } catch (...) {
        cs = connected_socket();
    }
    if (!cs) {
        throw std::runtime_error("Failed to connect (single attempt)");
    }
    auto c = make_connection(std::move(cs), cfg);
    std::exception_ptr ep;
    try {
        co_await c->startup();
        if (cfg.workload != "connect") {
            co_await do_request(*c, cfg);
        }
    } catch (...) {
        ep = std::current_exception();
    }
    co_await c->stop();
    if (ep) {
        std::rethrow_exception(ep);
    }
}

// Poll the REST API /compaction_manager/compactions until it returns an empty JSON array
// indicating there are no ongoing compactions. Throws on timeout.
static void wait_for_compactions(const raw_cql_test_config& cfg) {
    using namespace std::chrono_literals;
    const unsigned max_attempts = 600; // ~60s
    bool announced = false;
    for (unsigned attempt = 0; attempt < max_attempts; ++attempt) {
        try {
            connected_socket http_cs = connect(socket_address{
                    net::inet_address{cfg.remote_host}, 10000}).get();
            input_stream<char> in = http_cs.input();
            output_stream<char> out = http_cs.output();
            sstring req = seastar::format("GET /compaction_manager/compactions HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\r\n", cfg.remote_host);
            out.write(req).get();
            out.flush().get();
            sstring resp;
            while (true) {
                auto buf = in.read().get();
                if (!buf) {
                    break;
                }
                resp.append(buf.get(), buf.size());
            }
            auto pos = resp.find("\r\n\r\n");
            if (pos != sstring::npos) {
                auto body = resp.substr(pos + 4);
                boost::algorithm::trim(body);
                if (body == "[]") {
                    if (attempt) {
                        std::cout << "Compactions drained after " << attempt << " polls" << std::endl;
                    }
                    return;
                } else if (!announced) {
                    std::cout << "Waiting for compactions to end..." << std::endl;
                    announced = true;
                }
            }
        } catch (...) {
            // Ignore and retry
        }
        sleep(100ms).get();
    }
    throw std::runtime_error("Timed out waiting for compactions to drain (endpoint did not return empty JSON array)");
}

// Thread-local connection pool state extracted so that initialization can be performed
// outside of the timed body passed to time_parallel (avoids depressing the first TPS sample).
static thread_local std::vector<std::unique_ptr<raw_cql_connection>> tl_conns;

static future<> prepare_thread_connections(const raw_cql_test_config& cfg) {
    SCYLLA_ASSERT(tl_conns.empty());
    tl_conns.reserve(cfg.connections_per_shard);
    for (unsigned i = 0; i < cfg.connections_per_shard; ++i) {
        connected_socket cs;
        for (int attempt = 0; attempt < 200; ++attempt) {
            try {
                cs = co_await connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port});
            } catch (...) {
                cs = connected_socket();
            }
            if (cs) {
                break;
            }
            co_await sleep(std::chrono::milliseconds(25));
        }
        if (!cs) {
            throw std::runtime_error("Failed to connect to native transport port");
        }
        auto c = make_connection(std::move(cs), cfg);
        co_await c->startup();
        tl_conns.push_back(std::move(c));
    }
}

static void prepopulate(const raw_cql_test_config& cfg) {
    try {
        if (cfg.create_non_superuser) {
            connected_socket superuser_cs;
            for (int attempt = 0; attempt < 200; ++attempt) {
                try {
                    superuser_cs = connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port}).get();
                } catch (...) {
                    superuser_cs = connected_socket();
                }
                if (superuser_cs) {
                    break;
                }
                sleep(std::chrono::milliseconds(25)).get();
            }
            if (!superuser_cs) {
                throw std::runtime_error("populate phase: failed to connect as superuser");
            }
            raw_cql_connection superuser_conn(std::move(superuser_cs), sstring(cfg.username), sstring(cfg.password));
            try {
                superuser_conn.startup().get();
                ensure_schema(superuser_conn, cfg).get();
                create_role_with_permissions(superuser_conn, non_superuser_name, non_superuser_password).get();
            } catch (...) {
                superuser_conn.stop().get();
                throw;
            }
            superuser_conn.stop().get();
            std::cout << "Created role '" << non_superuser_name << "' with SELECT, MODIFY permissions on benchks.data" << std::endl;
        }

        connected_socket cs;
        for (int attempt = 0; attempt < 200; ++attempt) {
            try {
                cs = connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port}).get();
            } catch (...) {
                cs = connected_socket();
            }
            if (cs) {
                break;
            }
            sleep(std::chrono::milliseconds(25)).get();
        }
        if (!cs) {
            throw std::runtime_error("populate phase: failed to connect");
        }
        auto conn = make_connection(std::move(cs), cfg);
        try {
            conn->startup().get();
            if (!cfg.create_non_superuser) {
                ensure_schema(*conn, cfg).get();
            }
            // Write all clustering rows per partition for realistic reads.
            for (uint64_t seq = 0; seq < cfg.partitions; ++seq) {
                for (unsigned ck = 0; ck < num_ck_rows; ++ck) {
                    conn->write_one_ck(seq, ck).get();
                }
            }
        } catch (...) {
            conn->stop().get();
            throw;
        }
        conn->stop().get();
        std::cout << "Pre-populated " << cfg.partitions << " partitions (" << cfg.partitions * num_ck_rows << " rows, ~"
                  << (cfg.partitions * num_ck_rows * (payload_size + map_entries * map_value_size) / (1024*1024)) << " MB)" << std::endl;
    } catch (...) {
        std::cerr << "Population failed: " << std::current_exception() << std::endl;
        throw;
    }
}

static void wait_for_cql(const raw_cql_test_config& cfg) {
    for (int attempt = 0; attempt < 3000; ++attempt) {
        try {
            auto cs = connect(socket_address{net::inet_address{cfg.remote_host}, cfg.port}).get();
            auto conn = make_connection(std::move(cs), cfg);
            conn->startup().get();
            conn->stop().get();
            return;
        } catch (...) {
            // not ready yet
        }
        sleep(std::chrono::milliseconds(100)).get();
        if (attempt >= 100 && attempt % 10 == 0) {
            std::cout << format("Retrying connect to cql port (attempt {})", attempt+1) << std::endl;
        }
    }
    throw std::runtime_error("Timed out waiting for cql port to become ready");
}

static void workload_main(const raw_cql_test_config& cfg, sharded<abort_source>* as) {
    fmt::print("Running test with config: {}\n", cfg);
    auto cleanup = defer([] {
        // Cleanup thread-local connections to avoid destruction issues at exit
        smp::invoke_on_all([] {
            return parallel_for_each(tl_conns, [](std::unique_ptr<raw_cql_connection>& c) {
                    return c->stop();
            }).then([] {
                tl_conns.clear();
            });
        }).get();
    });
    wait_for_cql(cfg);
    if (cfg.workload != "connect") {
        prepopulate(cfg);
    }
    try {
        wait_for_compactions(cfg);
    } catch (...) {
        std::cerr << "Compaction wait failed: " << std::current_exception() << std::endl;
        throw;
    }
    if (!cfg.connection_per_request && cfg.workload != "connect") {
        // Warm up: establish all per-thread connections before measurement.
        try {
            auto shared_cfg = make_lw_shared<raw_cql_test_config>(cfg);
            smp::invoke_on_all([shared_cfg] {
                return prepare_thread_connections(*shared_cfg);
            }).get();
        } catch (...) {
            std::cerr << "Connection preparation failed: " << std::current_exception() << std::endl;
            throw;
        }
    }
    auto results = time_parallel([cfg, as] () -> future<> {
        as->local().check();
        if (cfg.connection_per_request || cfg.workload == "connect") {
            co_await run_one_with_new_connection(cfg);
        } else {
            static thread_local size_t idx = 0;
            // Round-robin over thread-local connections
            auto& c = *tl_conns[idx++ % tl_conns.size()];
            co_await do_request(c, cfg);
        }
    }, cfg.concurrency_per_connection * cfg.connections_per_shard, cfg.duration_in_seconds, cfg.operations_per_shard, !cfg.continue_after_error);
    aggregated_perf_results agg(results);
    std::cout << agg << std::endl;
    if (!cfg.json_result_file.empty()) {
        Json::Value params;
        params["workload"] = cfg.workload;
        params["partitions"] = cfg.partitions;
        params["replication"] = cfg.replication;
        params["duration"] = cfg.duration_in_seconds;
        params["operations_per_shard"] = cfg.operations_per_shard;
        params["concurrency_per_connection"] = cfg.concurrency_per_connection;
        params["connections_per_shard"] = cfg.connections_per_shard;
        params["username"] = cfg.username;
        params["remote_host"] = cfg.remote_host;
        params["connection_per_request"] = cfg.connection_per_request;
        params["create_non_superuser"] = cfg.create_non_superuser;
        params["cpus"] = smp::count;

        perf::write_json_result(cfg.json_result_file, agg, params, cfg.workload);
    }
}

// Returns a function which launches a performance workload that
// talks to the embedded server over the native CQL protocol using
// handcrafted CQL binary frames (no driver). Similar to perf_alternator
// (runs inside the server process) and perf_simple_query (similar workload types), but
// exercises the full networking + protocol parsing path.
//
// Example usage:
// ./build/release/scylla perf-cql-raw --workdir /tmp/scylla-workdir --smp 1 --cpus 0 --developer-mode 1 --workload read 2> /dev/null
std::function<int(int, char**)> perf_cql_raw(std::function<int(int, char**)> scylla_main, std::function<future<>(lw_shared_ptr<db::config>, sharded<abort_source>& as)>* after_init_func) {
    return [=](int ac, char** av) -> int {
        raw_cql_test_config c;
        bpo::options_description opts_desc;
        opts_desc.add_options()
            ("workload", bpo::value<std::string>()->default_value("read"), "workload type: read|write|connect")
            ("partitions", bpo::value<unsigned>()->default_value(10000), "number of partitions")
            ("duration", bpo::value<unsigned>()->default_value(5), "test duration seconds")
            ("operations-per-shard", bpo::value<unsigned>()->default_value(0), "fixed op count per shard")
            ("concurrency-per-shard", bpo::value<unsigned>()->default_value(10), "concurrent requests per connection")
            ("connections-per-shard", bpo::value<unsigned>()->default_value(100), "connections per shard")
            ("continue-after-error", bpo::value<bool>()->default_value(false), "continue after error")
            ("username", bpo::value<std::string>()->default_value(""), "authentication username (used as superuser when create-non-superuser is set)")
            ("password", bpo::value<std::string>()->default_value(""), "authentication password (used as superuser when create-non-superuser is set)")
            ("create-non-superuser", bpo::value<bool>()->default_value(false), "create a non-superuser role using username/password as superuser credentials")
            ("remote-host", bpo::value<std::string>()->default_value(""), "remote host to connect to, leave empty to run in-process server")
            ("connection-per-request", bpo::value<bool>()->default_value(false), "create a fresh connection for every request")
            ("replication", bpo::value<std::string>()->default_value("simple"), "replication strategy: simple (RF=1) or nts (NTS AWS_US_WEST_2:3)")
            ("json-result", bpo::value<std::string>()->default_value(""), "file to write json results to");
        bpo::variables_map vm;
        bpo::store(bpo::command_line_parser(ac,av).options(opts_desc).allow_unregistered().run(), vm);

        c.workload = vm["workload"].as<std::string>();
        c.partitions = vm["partitions"].as<unsigned>();
        c.duration_in_seconds = vm["duration"].as<unsigned>();
        c.operations_per_shard = vm["operations-per-shard"].as<unsigned>();
        c.concurrency_per_connection = vm["concurrency-per-shard"].as<unsigned>();
        c.connections_per_shard = vm["connections-per-shard"].as<unsigned>();
        c.continue_after_error = vm["continue-after-error"].as<bool>();
        c.username = vm["username"].as<std::string>();
        c.password = vm["password"].as<std::string>();
        c.create_non_superuser = vm["create-non-superuser"].as<bool>();
        c.remote_host = vm["remote-host"].as<std::string>();
        c.connection_per_request = vm["connection-per-request"].as<bool>();
        c.replication = vm["replication"].as<std::string>();
        c.json_result_file = vm["json-result"].as<std::string>();

        if (!c.username.empty() && c.password.empty()) {
            std::cerr << "--username specified without --password" << std::endl;
            return 1;
        }
        if (c.create_non_superuser && (c.username.empty() || c.password.empty())) {
            std::cerr << "--create-non-superuser requires both --username and --password" << std::endl;
            return 1;
        }
        if (c.workload != "read" && c.workload != "write" && c.workload != "connect") {
            std::cerr << "Unknown workload: " << c.workload << "\n"; return 1;
        }
        if (c.replication != "simple" && c.replication != "nts") {
            std::cerr << "Unknown replication: " << c.replication << " (expected 'simple' or 'nts')\n"; return 1;
        }

        // Remove test options to not disturb scylla main app
        for (auto& opt : opts_desc.options()) {
            auto name = opt->canonical_display_name(bpo::command_line_style::allow_long);
            std::tie(ac, av) = cut_arg(ac, av, name);
        }

        if (!c.remote_host.empty()) {
            // if remote-host provided (non-empty) we run standalone
            c.port = 9042; // TODO: make configurable
            app_template app;
            return app.run(ac, av, [c = std::move(c)] () -> future<> {
                return run_standalone([c = std::move(c)] (sharded<abort_source>* as) {
                    workload_main(c, as);
                });
            });
        }

        *after_init_func = [c](lw_shared_ptr<db::config> cfg, sharded<abort_source>& as) mutable {
            c.port = cfg->native_transport_port();
            c.remote_host = cfg->api_address();
            // run workload in background-ish
            return seastar::async([c, &as]() {
                try {
                    workload_main(c, &as);
                } catch (...) {
                    std::cerr << "Perf test failed: " << std::current_exception() << std::endl;
                    raise(SIGKILL); // abnormal shutdown to signal test failure
                }
                raise(SIGINT); // normal shutdown request after test completion
            });
        };
        return scylla_main(ac, av);
    };
}

} // namespace perf
