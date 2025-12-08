"""
Worker administration endpoints for adding, restarting, and removing workers.
"""
import logging
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["worker"])

# Security
security = HTTPBearer()

# Dependencies injected at runtime
_verify_api_key = None
_workers: Dict[str, dict] = None
_min_port: int = None
_max_port: int = None
_get_worker_id = None
_init_worker = None
_restart_worker = None
_remove_worker = None


def init_router(
    verify_api_key_func,
    workers_dict: Dict[str, dict],
    min_port: int,
    max_port: int,
    get_worker_id_func,
    init_worker_func,
    restart_worker_func,
    remove_worker_func,
):
    """
    Initialize the router with dependencies from the main controller.

    Args:
        verify_api_key_func: Function to verify API key
        workers_dict: Reference to workers dict
        min_port: Minimum worker port number
        max_port: Maximum worker port number
        get_worker_id_func: Function to get worker ID from port
        init_worker_func: Async function to initialize a worker
        restart_worker_func: Async function to restart a worker
        remove_worker_func: Async function to remove a worker
    """
    global _verify_api_key, _workers, _min_port, _max_port
    global _get_worker_id, _init_worker, _restart_worker, _remove_worker

    _verify_api_key = verify_api_key_func
    _workers = workers_dict
    _min_port = min_port
    _max_port = max_port
    _get_worker_id = get_worker_id_func
    _init_worker = init_worker_func
    _restart_worker = restart_worker_func
    _remove_worker = remove_worker_func


# =============================================================================
# WORKER ADMIN ENDPOINTS
# =============================================================================

@router.post("/worker/add")
async def add_worker(
    port: Optional[int] = None,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Add a new worker."""
    await _verify_api_key(credentials)

    if port is None:
        used_ports = {w["port"] for w in _workers.values()}
        for p in range(_min_port, _max_port + 1):
            if p not in used_ports:
                port = p
                break
        if port is None:
            raise HTTPException(status_code=503, detail="No available ports")

    worker_id = _get_worker_id(port)
    if worker_id in _workers:
        raise HTTPException(status_code=409, detail=f"Worker {worker_id} already exists")

    await _init_worker(port)
    return {"worker_id": worker_id, "port": port, "added": True}


@router.post("/worker/{worker_id}/restart")
async def restart_worker_endpoint(
    worker_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Restart a specific worker."""
    await _verify_api_key(credentials)

    if worker_id not in _workers:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    success = await _restart_worker(worker_id, reason="manual")
    return {"worker_id": worker_id, "restarted": success}


@router.post("/worker/{worker_id}/remove")
async def remove_worker_endpoint(
    worker_id: str,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Remove a worker."""
    await _verify_api_key(credentials)

    if worker_id not in _workers:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    success = await _remove_worker(worker_id)
    return {"worker_id": worker_id, "removed": success}
