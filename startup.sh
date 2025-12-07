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

if [ "$NUM_WORKERS" -gt 0 ]; then
    echo "  Starting $NUM_WORKERS workers on ports $WORKER_START_PORT-$((WORKER_START_PORT + NUM_WORKERS - 1))"

    # Start workers
    for i in $(seq 0 $((NUM_WORKERS - 1))); do
        port=$((WORKER_START_PORT + i))
        echo "Starting worker on port $port..."

        DOCLING_SERVE_ENG_LOC_NUM_WORKERS=$THREADS_PER_WORKER \
        docling-serve run --host 0.0.0.0 --port $port &

        # Small delay to avoid thundering herd on model loading
        sleep 1
    done

    echo "All workers started. Waiting for them to be ready..."
    sleep 10
else
    echo "  NUM_WORKERS=0: No workers started. Use /add endpoint to add workers."
fi

echo "Starting controller API on port 8000..."
cd /app && python -m uvicorn controller:app --host 0.0.0.0 --port 8000 &

# Start Cloudflare Tunnel if cert exists
if [ -f /app/cert.pem ]; then
    echo "Starting Cloudflare Tunnel (api.rundocling.com)..."
    # Extract token from PEM file (strip headers) and run tunnel
    TUNNEL_TOKEN=$(grep -v "ARGO TUNNEL TOKEN" /app/cert.pem | tr -d '\n')
    cloudflared tunnel run --token "$TUNNEL_TOKEN" &
fi

echo "=== Startup complete ==="
echo "Controller: http://0.0.0.0:8000 (local)"
echo "Public URL: https://api.rundocling.com"
echo "Log file: /data/controller.log"
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
