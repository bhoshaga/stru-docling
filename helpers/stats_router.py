"""
Stats and status endpoints for the controller.
"""
import logging
import time

import psutil
from fastapi import APIRouter, Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from . import job_manager, load_balancer, result_storage

logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["stats"])

# Security
security = HTTPBearer()

# These will be injected by init_router
_verify_api_key = None
_get_workers = None
_get_start_time = None


def init_router(
    verify_api_key_func,
    get_workers_func,
    get_start_time_func,
):
    """
    Initialize the router with dependencies from the main controller.

    Args:
        verify_api_key_func: Function to verify API key from credentials
        get_workers_func: Function to get workers dict
        get_start_time_func: Function to get app start time
    """
    global _verify_api_key, _get_workers, _get_start_time

    _verify_api_key = verify_api_key_func
    _get_workers = get_workers_func
    _get_start_time = get_start_time_func


# =============================================================================
# STATS ENDPOINTS
# =============================================================================


@router.get("/stats")
async def get_stats(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get processing statistics."""
    await _verify_api_key(credentials)

    job_stats = job_manager.get_stats()
    storage_stats = result_storage.get_storage_stats()
    pool_status = load_balancer.get_pool_status()

    start_time = _get_start_time()
    uptime = time.time() - start_time if start_time else 0

    return {
        "jobs": job_stats,
        "storage": storage_stats,
        "workers": pool_status,
        "uptime": uptime,
    }


@router.get("/status")
async def get_status(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Get status of all workers."""
    await _verify_api_key(credentials)

    workers = _get_workers()
    worker_list = []

    for worker_id, worker in workers.items():
        pid = worker["pid"]
        running = pid is not None and psutil.pid_exists(pid)
        worker_list.append({
            "worker_id": worker_id,
            "port": worker["port"],
            "pid": pid,
            "running": running,
            "state": worker["state"],
            "current_life_pages": worker.get("current_life_pages", 0),
            "lifetime_pages": worker.get("lifetime_pages", 0),
            "restart_count": worker.get("restart_count", 0),
            "last_restart_at": worker.get("last_restart_at"),
            "last_restart_reason": worker.get("last_restart_reason"),
            "current_job_id": worker.get("current_job_id"),
            "memory_mb": worker.get("memory_mb", 0),
        })

    return {
        "workers": sorted(worker_list, key=lambda w: w["port"]),
        "total": len(workers),
        "running": sum(1 for w in worker_list if w["running"]),
    }
