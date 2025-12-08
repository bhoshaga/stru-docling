#!/bin/bash
#
# Docling Multi-Worker Startup Script
#
# Launches docling-serve workers and the controller API (port 8000)
# Workers can be added/removed dynamically via the controller API
#
# Environment variables:
#   NUM_WORKERS - Number of workers to start (default: 10, set to 0 for manual control)
#   WORKER_START_PORT - First worker port (default: 5001)
#   DOCLING_SERVE_ENG_LOC_NUM_WORKERS - Threads per worker (default: 2)
#   DOCLING_DEVICE - Device to use: cuda or cpu (default: cuda)
#

set -e

NUM_WORKERS=${NUM_WORKERS:-10}
WORKER_START_PORT=${WORKER_START_PORT:-5001}
THREADS_PER_WORKER=${DOCLING_SERVE_ENG_LOC_NUM_WORKERS:-2}

echo "=== Docling Controller Startup ==="
echo "  Device: ${DOCLING_DEVICE:-cpu}"
echo "  Threads per worker: $THREADS_PER_WORKER"

# Start Cloudflare Tunnel early (doesn't depend on workers)
# Token can be passed via TUNNEL_TOKEN env var or /app/cert.pem file
if [ -n "$TUNNEL_TOKEN" ]; then
    echo "Starting Cloudflare Tunnel (from env var)..."
    cloudflared tunnel --url http://localhost:8000 run --token "$TUNNEL_TOKEN" &
elif [ -f /app/cert.pem ]; then
    echo "Starting Cloudflare Tunnel (from cert.pem)..."
    TUNNEL_TOKEN=$(cat /app/cert.pem | tr -d '\n')
    cloudflared tunnel --url http://localhost:8000 run --token "$TUNNEL_TOKEN" &
fi

if [ "$NUM_WORKERS" -gt 0 ]; then
    echo "  Starting $NUM_WORKERS workers on ports $WORKER_START_PORT-$((WORKER_START_PORT + NUM_WORKERS - 1))"

    # Start workers
    for i in $(seq 0 $((NUM_WORKERS - 1))); do
        port=$((WORKER_START_PORT + i))
        echo "Starting worker on port $port..."

        DOCLING_SERVE_ENG_LOC_NUM_WORKERS=$THREADS_PER_WORKER \
        docling-serve run --host 0.0.0.0 --port $port &
    done

    echo "All workers started."
else
    echo "  NUM_WORKERS=0: No workers started. Use /add endpoint to add workers."
fi

echo "Starting controller API on port 8000..."
cd /app && python -m uvicorn controller:app --host 0.0.0.0 --port 8000 &

# Wait for all workers to be ready
echo "Waiting for workers to be ready..."
workers_ready=""
while true; do
    ready=0
    for i in $(seq 0 $((NUM_WORKERS - 1))); do
        port=$((WORKER_START_PORT + i))
        if curl -s http://localhost:$port/health > /dev/null 2>&1; then
            ready=$((ready + 1))
            if ! echo "$workers_ready" | grep -q ":$port:"; then
                echo "  Worker $port ready"
                workers_ready="$workers_ready:$port:"
            fi
        fi
    done
    if [ $ready -eq $NUM_WORKERS ]; then
        break
    fi
    sleep 1
done

echo "Server ready."
echo ""
echo "Controller: http://0.0.0.0:8000 (local)"
echo "Public URL: https://api.rundocling.com"
echo "Log file: /app/server.log"
echo "Endpoints:"
echo "  POST /v1/convert/file/async     - Convert PDF with page splitting"
echo "  GET  /v1/status/poll/{id}       - Poll job status"
echo "  GET  /v1/result/{id}            - Get job result"
echo "  GET  /v1/*                      - Proxy to worker (round-robin)"
echo "  GET  /stats                     - Processing stats"
echo "  GET  /health                    - Worker health"
echo "  POST /worker/add                - Add worker"
echo "  POST /worker/{id}/restart       - Restart worker"
echo "  POST /worker/{id}/remove        - Remove worker"
echo "API Keys: windowseat (dedicated), middleseat (shared)"

# Keep container running and forward signals
wait
