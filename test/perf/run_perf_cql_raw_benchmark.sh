#!/bin/bash
#
# run_perf_cql_raw_benchmark.sh
#
# Starts a 3-node ScyllaDB cluster locally and runs the perf-cql-raw benchmark
# against it using the benchks.data schema (~1MB rows). Each node is sized to emulate
# an i4i.xlarge with 2 cores (--smp 2, ~15G memory).
#
# All output is logged to a testlog/ directory.
#
# Usage:
#   ./test/perf/run_perf_cql_raw_benchmark.sh [options]
#
# Options:
#   --workload <read|write>   Benchmark workload (default: read)
#   --partitions <N>          Number of partitions to pre-populate (default: 10000)
#   --duration <seconds>      Benchmark duration per iteration (default: 300)
#   --concurrency <N>         Concurrent requests per connection (default: 100)
#   --connections <N>         Connections per shard (default: 10)
#   --skip-build              Skip building the scylla binary
#   --scylla-path <path>      Path to scylla binary (default: build/release/scylla)
#   --smp <N>                 Number of cores per node (default: 2)
#   --memory <size>           Memory per node, e.g. 15G (default: 15G)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# -------------------------------------------------------------------
# Defaults
# -------------------------------------------------------------------
WORKLOAD="read"
PARTITIONS=1000
DURATION=300
CONCURRENCY=10
CONNECTIONS=1000
SKIP_BUILD=false
SCYLLA_PATH="$REPO_ROOT/build/release/scylla"
NODE_SMP=1
NODE_MEMORY="1G"
LOADER_SMP=1

# -------------------------------------------------------------------
# Parse arguments
# -------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --workload)       WORKLOAD="$2"; shift 2 ;;
        --partitions)     PARTITIONS="$2"; shift 2 ;;
        --duration)       DURATION="$2"; shift 2 ;;
        --concurrency)    CONCURRENCY="$2"; shift 2 ;;
        --connections)    CONNECTIONS="$2"; shift 2 ;;
        --skip-build)     SKIP_BUILD=true; shift ;;
        --scylla-path)    SCYLLA_PATH="$2"; shift 2 ;;
        --smp)            NODE_SMP="$2"; shift 2 ;;
        --memory)         NODE_MEMORY="$2"; shift 2 ;;
        *)                echo "Unknown option: $1"; exit 1 ;;
    esac
done

# -------------------------------------------------------------------
# Directories
# -------------------------------------------------------------------
LOGDIR="$REPO_ROOT/testlog"
TMPDIR_BASE=$(mktemp -d /tmp/perf-cql-raw-XXXXXX)

mkdir -p "$LOGDIR"

echo "=== perf-cql-raw benchmark ==="
echo "  Workload:       $WORKLOAD"
echo "  Partitions:     $PARTITIONS"
echo "  Duration:       ${DURATION}s"
echo "  Concurrency:    $CONCURRENCY"
echo "  Connections:    $CONNECTIONS"
echo "  Node SMP:       $NODE_SMP"
echo "  Node Memory:    $NODE_MEMORY"
echo "  Loader SMP:     $LOADER_SMP"
echo "  Log dir:        $LOGDIR"
echo "  Temp dir:       $TMPDIR_BASE"
echo ""

# -------------------------------------------------------------------
# Node addresses — use loopback aliases (127.0.0.{1,2,3})
# -------------------------------------------------------------------
NODE_ADDRS=("127.0.0.1" "127.0.0.2" "127.0.0.3")
SEED="${NODE_ADDRS[0]}"
NUM_NODES=${#NODE_ADDRS[@]}

# Unique API ports per node to avoid conflicts (all share CQL port 9042).
API_PORTS=(10000 10001 10002)

# -------------------------------------------------------------------
# Build
# -------------------------------------------------------------------
if [ "$SKIP_BUILD" = false ]; then
    echo ">>> Building release binary..."
    (cd "$REPO_ROOT" && ninja build/release/scylla) 2>&1 | tee "$LOGDIR/build.log"
    echo ">>> Build complete."
else
    echo ">>> Skipping build (--skip-build)."
fi

if [ ! -x "$SCYLLA_PATH" ]; then
    echo "ERROR: scylla binary not found at $SCYLLA_PATH"
    exit 1
fi

# -------------------------------------------------------------------
# Prepare per-node SCYLLA_HOME with DC/rack properties
# -------------------------------------------------------------------
# All nodes go into the AWS_US_WEST_2 datacenter so that the NTS
# keyspace with RF=3 can satisfy quorum on this 3-node local cluster.
SCYLLA_HOME_DIR="$TMPDIR_BASE/scylla_home"
mkdir -p "$SCYLLA_HOME_DIR/conf"
cat > "$SCYLLA_HOME_DIR/conf/cassandra-rackdc.properties" <<'EOF'
dc=AWS_US_WEST_2
rack=RACK1
EOF

# Copy the default scylla.yaml so the nodes have a valid base config.
cp "$REPO_ROOT/conf/scylla.yaml" "$SCYLLA_HOME_DIR/conf/scylla.yaml"

# -------------------------------------------------------------------
# Cleanup handler
# -------------------------------------------------------------------
NODE_PIDS=()

cleanup() {
    echo ""
    echo ">>> Cleaning up..."
    for pid in "${NODE_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "    Stopping node PID $pid"
            kill "$pid" 2>/dev/null || true
        fi
    done
    # Wait for nodes to exit (with timeout)
    for pid in "${NODE_PIDS[@]}"; do
        local timeout=30
        while kill -0 "$pid" 2>/dev/null && [ $timeout -gt 0 ]; do
            sleep 1
            timeout=$((timeout - 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            echo "    Force-killing node PID $pid"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    echo "    Removing temp dir $TMPDIR_BASE"
    rm -rf "$TMPDIR_BASE"
    echo ">>> Done."
}

trap cleanup EXIT
trap 'echo ""; echo ">>> Interrupted (Ctrl+C)."; exit 130' INT TERM

# -------------------------------------------------------------------
# Start cluster
# -------------------------------------------------------------------
echo ">>> Starting $NUM_NODES-node cluster..."

for i in $(seq 0 $((NUM_NODES - 1))); do
    addr="${NODE_ADDRS[$i]}"
    api_port="${API_PORTS[$i]}"
    workdir="$TMPDIR_BASE/node$((i + 1))"
    logfile="$LOGDIR/node$((i + 1)).log"

    mkdir -p "$workdir"

    echo "    Starting node $((i + 1)) on $addr (API port $api_port, workdir $workdir)..."

    SCYLLA_HOME="$SCYLLA_HOME_DIR" "$SCYLLA_PATH" \
        --workdir "$workdir" \
        --smp "$NODE_SMP" \
        --memory "$NODE_MEMORY" \
        --developer-mode 1 \
        --overprovisioned \
        --unsafe-bypass-fsync 1 \
        --listen-address "$addr" \
        --rpc-address "$addr" \
        --api-address "$addr" \
        --api-port "$api_port" \
        --prometheus-address "$addr" \
        --seed-provider-parameters "seeds=$SEED" \
        --endpoint-snitch GossipingPropertyFileSnitch \
        --ring-delay-ms 0 \
        > "$logfile" 2>&1 &

    NODE_PIDS+=($!)
    echo "    Node $((i + 1)) PID: ${NODE_PIDS[$i]}"
done

# -------------------------------------------------------------------
# Wait for all nodes to be ready (CQL port open)
# -------------------------------------------------------------------
echo ""
echo ">>> Waiting for all nodes to become ready..."

wait_for_cql_port() {
    local addr="$1"
    local port=9042
    local max_wait=300  # 5 minutes
    local elapsed=0
    while ! bash -c "echo > /dev/tcp/$addr/$port" 2>/dev/null; do
        sleep 1
        elapsed=$((elapsed + 1))
        if [ $elapsed -ge $max_wait ]; then
            echo "ERROR: Node $addr did not open CQL port within ${max_wait}s"
            return 1
        fi
        if [ $((elapsed % 30)) -eq 0 ]; then
            echo "    Still waiting for $addr:$port ($elapsed seconds)..."
        fi
    done
    echo "    Node $addr:$port is ready (${elapsed}s)"
}

for addr in "${NODE_ADDRS[@]}"; do
    wait_for_cql_port "$addr"
done

echo ">>> All nodes ready."
echo ""

# -------------------------------------------------------------------
# Run benchmark
# -------------------------------------------------------------------
echo ">>> Running perf-cql-raw benchmark (workload=$WORKLOAD, duration=${DURATION}s)..."
echo "    Log: $LOGDIR/benchmark.log"
echo "    Results: $LOGDIR/results.json"
echo ""

"$SCYLLA_PATH" perf-cql-raw \
    --remote-host "$SEED" \
    --workload "$WORKLOAD" \
    --partitions "$PARTITIONS" \
    --duration "$DURATION" \
    --concurrency-per-shard "$CONCURRENCY" \
    --connections-per-shard "$CONNECTIONS" \
    --replication nts \
    --continue-after-error true \
    --json-result "$LOGDIR/results.json" \
    --smp "$LOADER_SMP" \
    --memory 2G \
    --overprovisioned \
    2>&1 | tee "$LOGDIR/benchmark.log"

echo ""
echo ">>> Benchmark complete."
echo "    Results:  $LOGDIR/results.json"
echo "    Logs:     $LOGDIR/"
