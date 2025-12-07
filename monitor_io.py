#!/usr/bin/env python3
"""
Monitor io.net container metrics over time.

Usage:
    python monitor_io.py <duration_seconds> <interval_seconds>

Examples:
    python monitor_io.py 30 1    # 30s, every 1s
    python monitor_io.py 60 5    # 60s, every 5s
"""

import argparse
import json
import os
import time
import traceback
import requests
from datetime import datetime

LOG_DIR = "/Users/bhoshaga/stru-docling/logs"

BASE_URL = "https://5f8789e25046-0eb95f5d-26c0-4a2c-9d1a-a654d2309097-caas.user-hosted-content.io.solutions"
API_KEY = "windowseat"

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json"
}


def exec_cmd(cmd):
    """Execute command on container via /exec endpoint. Returns (stdout, error_traceback)."""
    try:
        resp = requests.post(
            f"{BASE_URL}/exec",
            headers=HEADERS,
            json={"cmd": cmd},
            timeout=10
        )
        resp.raise_for_status()
        return resp.json().get("stdout", ""), None
    except Exception:
        return None, traceback.format_exc()


def get_system_metrics():
    """Get system metrics via /exec commands."""
    metrics = {"errors": [], "tracebacks": []}

    # CPU and load average from /proc/loadavg
    loadavg, err = exec_cmd("cat /proc/loadavg")
    if err:
        metrics["errors"].append("loadavg")
        metrics["tracebacks"].append(f"[loadavg] {err}")
    elif loadavg:
        try:
            parts = loadavg.strip().split()
            if len(parts) >= 3:
                metrics["load_1m"] = float(parts[0])
                metrics["load_5m"] = float(parts[1])
                metrics["load_15m"] = float(parts[2])
        except Exception:
            metrics["errors"].append("loadavg_parse")
            metrics["tracebacks"].append(f"[loadavg_parse] {traceback.format_exc()}")

    # Memory from free
    mem_output, err = exec_cmd("free -b | grep Mem")
    if err:
        metrics["errors"].append("memory")
        metrics["tracebacks"].append(f"[memory] {err}")
    elif mem_output:
        try:
            parts = mem_output.split()
            if len(parts) >= 3:
                total = int(parts[1])
                used = int(parts[2])
                if total > 0:
                    metrics["mem_total_gb"] = round(total / (1024**3), 2)
                    metrics["mem_used_gb"] = round(used / (1024**3), 2)
                    metrics["mem_percent"] = round(used / total * 100, 1)
        except Exception:
            metrics["errors"].append("memory_parse")
            metrics["tracebacks"].append(f"[memory_parse] {traceback.format_exc()}")

    # CPU usage from /proc/stat (instant snapshot)
    cpu_output, err = exec_cmd("head -1 /proc/stat")
    if err:
        metrics["errors"].append("cpu")
        metrics["tracebacks"].append(f"[cpu] {err}")
    elif cpu_output:
        try:
            parts = cpu_output.split()[1:]  # skip 'cpu' label
            if len(parts) >= 7:
                vals = [int(x) for x in parts[:7]]
                total = sum(vals)
                idle = vals[3]
                metrics["cpu_total_ticks"] = total
                metrics["cpu_idle_ticks"] = idle
        except Exception:
            metrics["errors"].append("cpu_parse")
            metrics["tracebacks"].append(f"[cpu_parse] {traceback.format_exc()}")

    # GPU from nvidia-smi
    gpu_output, err = exec_cmd("nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null")
    if err:
        metrics["errors"].append("gpu")
        metrics["tracebacks"].append(f"[gpu] {err}")
    elif gpu_output and gpu_output.strip():
        try:
            parts = [x.strip() for x in gpu_output.strip().split(",")]
            if len(parts) >= 4:
                metrics["gpu_util"] = int(parts[0])
                metrics["gpu_mem_used_mb"] = int(parts[1])
                metrics["gpu_mem_total_mb"] = int(parts[2])
                metrics["gpu_temp_c"] = int(parts[3])
        except Exception:
            metrics["errors"].append("gpu_parse")
            metrics["tracebacks"].append(f"[gpu_parse] {traceback.format_exc()}")

    # Process count
    proc_output, err = exec_cmd("ps aux | wc -l")
    if err:
        metrics["errors"].append("proc")
        metrics["tracebacks"].append(f"[proc] {err}")
    elif proc_output and proc_output.strip():
        try:
            metrics["proc_count"] = int(proc_output.strip()) - 1  # minus header
        except Exception:
            pass

    return metrics


def get_worker_health():
    """Get worker health from /health endpoint."""
    try:
        resp = requests.get(f"{BASE_URL}/health", headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        workers = data.get("workers", [])
        return {
            "total": len(workers),
            "healthy": sum(1 for w in workers if w.get("healthy")),
            "mem_mb": sum(w.get("memory_mb", 0) for w in workers),
            "traceback": None
        }
    except Exception:
        return {"total": 0, "healthy": 0, "mem_mb": 0, "traceback": traceback.format_exc()}


def format_metrics(sys_metrics, worker_info):
    """Format metrics for display."""
    parts = []

    # Check for errors
    if sys_metrics.get("errors"):
        parts.append(f"SYS_ERR: {','.join(sys_metrics['errors'])}")
    if worker_info.get("traceback"):
        parts.append("WORKER_ERR")

    # Load average
    if "load_1m" in sys_metrics:
        parts.append(f"Load: {sys_metrics['load_1m']:.1f}/{sys_metrics.get('load_5m', 0):.1f}/{sys_metrics.get('load_15m', 0):.1f}")
    else:
        parts.append("Load: N/A")

    # Memory
    if "mem_used_gb" in sys_metrics:
        parts.append(f"Mem: {sys_metrics['mem_used_gb']:.1f}/{sys_metrics.get('mem_total_gb', 0):.1f}GB ({sys_metrics.get('mem_percent', 0):.0f}%)")
    else:
        parts.append("Mem: N/A")

    # GPU
    if "gpu_util" in sys_metrics:
        parts.append(f"GPU: {sys_metrics['gpu_util']}%")
        parts.append(f"VRAM: {sys_metrics.get('gpu_mem_used_mb', 0)}/{sys_metrics.get('gpu_mem_total_mb', 0)}MB")
        parts.append(f"Temp: {sys_metrics.get('gpu_temp_c', 0)}C")
    else:
        parts.append("GPU: N/A")

    # Workers
    parts.append(f"Workers: {worker_info.get('healthy', 0)}/{worker_info.get('total', 0)}")

    return " | ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Monitor io.net container metrics")
    parser.add_argument("duration", type=float, help="Total duration in seconds")
    parser.add_argument("interval", type=float, help="Interval between samples in seconds")
    args = parser.parse_args()

    samples = []
    start_time = time.time()
    end_time = start_time + args.duration
    sample_count = 0

    print(f"Monitoring for {args.duration}s every {args.interval}s...")
    print("-" * 120)

    try:
        while time.time() < end_time:
            sample_start = time.time()

            sys_metrics = get_system_metrics()
            worker_info = get_worker_health()
            timestamp = datetime.now().isoformat()

            sample = {
                "timestamp": timestamp,
                "elapsed_s": round(time.time() - start_time, 2),
                "system": sys_metrics,
                "workers": worker_info
            }
            samples.append(sample)
            sample_count += 1

            # Print formatted output
            print(f"[{sample_count:3d}] {timestamp[11:19]} | {format_metrics(sys_metrics, worker_info)}")

            # Wait for next interval
            elapsed = time.time() - sample_start
            sleep_time = max(0, args.interval - elapsed)
            if time.time() + sleep_time < end_time:
                time.sleep(sleep_time)
            else:
                break

    except KeyboardInterrupt:
        print("\nInterrupted by user")

    print("-" * 120)
    print(f"Collected {len(samples)} samples over {time.time() - start_time:.1f}s")

    # Always save to log directory
    os.makedirs(LOG_DIR, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(LOG_DIR, f"metrics_{timestamp_str}.json")
    with open(log_file, "w") as f:
        json.dump(samples, f, indent=2)
    print(f"Saved to {log_file}")


if __name__ == "__main__":
    main()
