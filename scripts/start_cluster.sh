#!/bin/bash
# ================================================================
#  Raft 3-Node Cluster Startup Script (Linux/macOS)
#
#  Starts 3 kv_raft nodes with Raft consensus on ports 8001-8003
#  and Redis-compatible RESP servers on ports 6379-6381.
#
#  Usage:
#    ./start_cluster.sh              # Start all 3 nodes
#    ./start_cluster.sh --stop       # Stop all running nodes
#    ./start_cluster.sh --status     # Check if nodes are running
# ================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BINARY="$PROJECT_DIR/build/kv_raft"

# Find the binary
if [ ! -f "$BINARY" ]; then
    BINARY="$PROJECT_DIR/build_sweep/kv_raft"
fi

# Cluster data directory
CLUSTER_DIR="$PROJECT_DIR/cluster"

# Node config
NODES=(
    "node1:8001:6379:9091"
    "node2:8002:6380:9092"
    "node3:8003:6381:9093"
)

PEERS="--peer node1:127.0.0.1:8001 --peer node2:127.0.0.1:8002 --peer node3:127.0.0.1:8003"

# PID files
PID_DIR="$CLUSTER_DIR/pids"
PID_FILES=(
    "$PID_DIR/node1.pid"
    "$PID_DIR/node2.pid"
    "$PID_DIR/node3.pid"
)

# ================================================================
#  Helper functions
# ================================================================

check_binary() {
    if [ ! -f "$BINARY" ]; then
        echo "ERROR: kv_raft not found at $BINARY"
        echo "Build with: cd $PROJECT_DIR && mkdir -p build && cd build && cmake .. && make -j\$(nproc)"
        exit 1
    fi
}

create_dirs() {
    mkdir -p "$CLUSTER_DIR/node1" "$CLUSTER_DIR/node2" "$CLUSTER_DIR/node3"
    mkdir -p "$PID_DIR"
}

start_node() {
    local name="$1"
    local raft_port="$2"
    local resp_port="$3"
    local metrics_port="$4"
    local pid_file="$5"

    local data_dir="$CLUSTER_DIR/$name"
    local log_file="$CLUSTER_DIR/${name}.log"

    echo "  Starting $name (raft=$raft_port, resp=$resp_port, metrics=$metrics_port)..."

    "$BINARY" \
        --id "$name" \
        --raft-port "$raft_port" \
        --resp-port "$resp_port" \
        --metrics-port "$metrics_port" \
        $PEERS \
        --data-dir "$data_dir" \
        > "$log_file" 2>&1 &

    echo $! > "$pid_file"
    echo "    PID: $(cat $pid_file)"
}

stop_nodes() {
    echo "Stopping all nodes..."
    for pid_file in "${PID_FILES[@]}"; do
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  Stopping PID $pid..."
                kill -TERM "$pid" 2>/dev/null || true
                # Wait for graceful shutdown
                for i in {1..10}; do
                    if ! kill -0 "$pid" 2>/dev/null; then
                        break
                    fi
                    sleep 0.5
                done
                # Force kill if still running
                if kill -0 "$pid" 2>/dev/null; then
                    echo "  Force killing PID $pid..."
                    kill -9 "$pid" 2>/dev/null || true
                fi
            fi
            rm -f "$pid_file"
        fi
    done
    echo "All nodes stopped."
}

check_status() {
    local any_running=0
    for pid_file in "${PID_FILES[@]}"; do
        local name=$(basename "$pid_file" .pid)
        if [ -f "$pid_file" ]; then
            local pid=$(cat "$pid_file")
            if kill -0 "$pid" 2>/dev/null; then
                echo "  $name: RUNNING (PID $pid)"
                any_running=1
            else
                echo "  $name: STOPPED (stale PID $pid)"
            fi
        else
            echo "  $name: NOT STARTED"
        fi
    done
    if [ $any_running -eq 0 ]; then
        echo "No nodes are running."
    fi
}

# ================================================================
#  Main
# ================================================================

case "${1:-}" in
    --stop)
        stop_nodes
        exit 0
        ;;
    --status)
        check_status
        exit 0
        ;;
    --help|-h)
        echo "Usage: $0 [--stop|--status|--help]"
        echo ""
        echo "  (no args)    Start all 3 cluster nodes"
        echo "  --stop       Stop all running nodes gracefully"
        echo "  --status     Show running status of each node"
        echo "  --help       Show this help"
        exit 0
        ;;
esac

check_binary
create_dirs

echo "========================================================"
echo "  Starting 3-Node Raft KV Cluster"
echo "========================================================"
echo ""
echo "  Node 1: raft=8001, resp=6379, metrics=9091"
echo "  Node 2: raft=8002, resp=6380, metrics=9092"
echo "  Node 3: raft=8003, resp=6381, metrics=9093"
echo ""
echo "  Logs: $CLUSTER_DIR/node*.log"
echo "  PIDs: $PID_DIR/node*.pid"
echo "========================================================"
echo ""

# Start all nodes in background
for i in "${!NODES[@]}"; do
    IFS=':' read -r name raft_port resp_port metrics_port <<< "${NODES[$i]}"
    start_node "$name" "$raft_port" "$resp_port" "$metrics_port" "${PID_FILES[$i]}"
done

echo ""
echo "All 3 nodes started."
echo ""
echo "Test with:"
echo "  redis-cli -h 127.0.0.1 -p 6379 PING"
echo "  redis-cli -h 127.0.0.1 -p 6379 SET hello world"
echo "  redis-cli -h 127.0.0.1 -p 6380 GET hello"
echo ""
echo "Metrics:"
echo "  http://127.0.0.1:9091/metrics"
echo "  http://127.0.0.1:9092/metrics"
echo "  http://127.0.0.1:9093/metrics"
echo ""
echo "Health:"
echo "  http://127.0.0.1:9091/health"
echo "  redis-cli -h 127.0.0.1 -p 6379 HEALTH"
echo ""
echo "Stop with: $0 --stop"
echo "Status:    $0 --status"