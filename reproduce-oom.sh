#!/bin/bash
set -e

SCYLLA=./build/release/scylla
WORKDIR=/tmp/scylla-workdir

# Kill any running scylla instances
pkill -9 -f 'scylla.*--workdir' 2>/dev/null || true
sleep 2

# Clean up previous working directory
rm -rf "$WORKDIR"

# Root cause: transport/server.cc line ~765:
#   auto mem_estimate = f.length * 2 + 8000;
# This underestimates unprepared statement memory (parsing, compilation, AST
# nodes, etc). The memory_limiter semaphore = available_memory/10, so with
# tight memory + high concurrency, the semaphore admits too many requests
# whose real cost exceeds the estimate => OOM.

echo "=== Starting Scylla server (256 MB RAM, 1 smp) ==="
$SCYLLA \
    --workdir "$WORKDIR" \
    --smp 1 \
    -m 256M \
    --developer-mode 1 \
    --overprovisioned \
    > scylla-server.log 2>&1 &
SCYLLA_PID=$!
echo "Scylla PID: $SCYLLA_PID"

# Wait for CQL port to be ready
echo "Waiting for Scylla to start listening on port 9042..."
for i in $(seq 1 60); do
    if ! kill -0 $SCYLLA_PID 2>/dev/null; then
        echo "ERROR: Scylla exited before becoming ready!"
        tail -20 scylla-server.log
        exit 1
    fi
    if ss -tlnp 2>/dev/null | grep -q ':9042 '; then
        echo "Scylla is ready (took ~${i}s)"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "ERROR: Timeout waiting for Scylla to start"
        tail -20 scylla-server.log
        kill $SCYLLA_PID 2>/dev/null || true
        exit 1
    fi
    sleep 1
done

# Unprepared-statement flood using WRITE workload with map literals.
# Each INSERT contains a map literal with 500 entries.
# Map literals are the most memory-expensive CQL construct: 3 expression
# allocations per entry (key + value + tuple wrapper).
#
# Wire size per query: ~20KB (500 entries * ~40 bytes each)
#   mem_estimate = 20000*2 + 8000 = 48KB
# Actual memory per query: 500 entries * 3 allocs * ~150 bytes = ~225KB
#   plus preparation doubling = ~450KB peak
# Ratio: ~9x underestimation
#
# Semaphore budget: 256MB/10 = 25.6MB
# Slots admitted: 25.6MB / 48KB ≈ 533 concurrent
# Actual peak: 533 * 450KB ≈ 234MB (on top of ~80MB baseline = ~314MB > 256MB)
echo "=== Starting unprepared-statement flood (write workload with map literals) ==="
$SCYLLA perf-cql-raw \
    --smp 1 \
    --workload write \
    --use-prepared 0 \
    --remote-host 127.0.0.1 \
    --connections-per-shard 2000 \
    --concurrency-per-shard 200 \
    --tables 10 \
    --partitions 1000 \
    --duration 30 \
    --continue-after-error 1 \
    2>&1 | tee perf-unprepared.log || true

echo ""
echo "=== Checking if Scylla survived ==="
if kill -0 $SCYLLA_PID 2>/dev/null; then
    echo "Scylla survived (no OOM triggered)."
    kill $SCYLLA_PID
    wait $SCYLLA_PID 2>/dev/null || true
else
    wait $SCYLLA_PID 2>/dev/null
    EXIT_CODE=$?
    echo "Scylla CRASHED (exit code: $EXIT_CODE) — likely OOM!"
    echo "--- Last 30 lines of scylla-server.log ---"
    tail -n 30 scylla-server.log
fi
