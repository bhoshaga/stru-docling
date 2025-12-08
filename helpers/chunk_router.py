"""
Chunk endpoints for hierarchical and hybrid document chunking.

These endpoints proxy chunk requests to workers.
"""
import logging
from io import BytesIO
from typing import Dict

import httpx
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["chunk"])

# Security
security = HTTPBearer()

# Dependencies injected at runtime
_verify_api_key = None
_check_file_limits = None
_get_available_workers = None
_load_balancer = None
_chunk_tasks: Dict[str, int] = None  # Reference to controller's chunk_tasks dict


def init_router(
    verify_api_key_func,
    check_file_limits_func,
    get_available_workers_func,
    load_balancer,
    chunk_tasks_dict: Dict[str, int],
):
    """
    Initialize the router with dependencies from the main controller.

    This avoids circular imports by injecting dependencies at runtime.

    Args:
        verify_api_key_func: Function to verify API key
        check_file_limits_func: Function to check file size/page limits
        get_available_workers_func: Async function to get available workers
        load_balancer: LoadBalancer instance
        chunk_tasks_dict: Dict to store task_id -> worker_port mappings
    """
    global _verify_api_key, _check_file_limits, _get_available_workers, _load_balancer, _chunk_tasks

    _verify_api_key = verify_api_key_func
    _check_file_limits = check_file_limits_func
    _get_available_workers = get_available_workers_func
    _load_balancer = load_balancer
    _chunk_tasks = chunk_tasks_dict


# =============================================================================
# FILE CHUNK HELPERS
# =============================================================================

async def _proxy_chunk_request(
    files: UploadFile,
    endpoint_path: str,
    api_key: str,
    is_async: bool = False,
) -> dict:
    """Proxy a chunk request to a single worker."""
    file_bytes = await files.read()
    filename = files.filename or "document.pdf"
    is_pdf = filename.lower().endswith(".pdf")

    # Check tier limits
    limit_error = _check_file_limits(file_bytes, api_key, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Get available workers (with detailed logging)
    available_workers = await _get_available_workers(api_key, max_wait=60)
    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available after 60s wait")

    # Use load balancer to pick best worker
    worker_port = _load_balancer.select_single_worker(api_key)
    if not worker_port:
        worker_port = available_workers[0]

    logger.info(f"[CHUNK] Proxying {endpoint_path} to worker {worker_port}")

    async with httpx.AsyncClient(timeout=300.0) as client:
        # Must wrap bytes in BytesIO for httpx async file uploads
        files_param = {"files": (filename, BytesIO(file_bytes), "application/octet-stream")}
        resp = await client.post(
            f"http://127.0.0.1:{worker_port}{endpoint_path}",
            files=files_param,
        )

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        result = resp.json()

        # For async requests, store task_id -> worker mapping for poll/result
        if is_async and "task_id" in result:
            _chunk_tasks[result["task_id"]] = worker_port
            logger.info(f"[CHUNK] Stored mapping: {result['task_id']} -> worker {worker_port}")

        return result


# =============================================================================
# SOURCE CHUNK HELPERS
# =============================================================================

async def _proxy_chunk_source_request(
    request: Request,
    endpoint_path: str,
    api_key: str,
    is_async: bool = False,
) -> dict:
    """Proxy a chunk source (URL) request to a single worker."""
    # Get available workers (with detailed logging)
    available_workers = await _get_available_workers(api_key, max_wait=60)
    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available after 60s wait")

    # Use load balancer to pick best worker
    worker_port = _load_balancer.select_single_worker(api_key)
    if not worker_port:
        worker_port = available_workers[0]

    logger.info(f"[CHUNK] Proxying source {endpoint_path} to worker {worker_port}")

    # Get JSON body
    body = await request.json()

    async with httpx.AsyncClient(timeout=300.0) as client:
        resp = await client.post(
            f"http://127.0.0.1:{worker_port}{endpoint_path}",
            json=body,
        )

        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)

        result = resp.json()

        # For async requests, store task_id -> worker mapping for poll/result
        if is_async and "task_id" in result:
            _chunk_tasks[result["task_id"]] = worker_port
            logger.info(f"[CHUNK] Stored mapping: {result['task_id']} -> worker {worker_port}")

        return result


# =============================================================================
# FILE CHUNK ENDPOINTS
# =============================================================================

# Hierarchical chunking - sync
@router.post("/v1/chunk/hierarchical/file")
async def chunk_hierarchical_file(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hierarchical chunking of a document (sync)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_request(files, "/v1/chunk/hierarchical/file", api_key)


# Hierarchical chunking - async
@router.post("/v1/chunk/hierarchical/file/async")
async def chunk_hierarchical_file_async(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hierarchical chunking of a document (async)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_request(files, "/v1/chunk/hierarchical/file/async", api_key, is_async=True)


# Hybrid chunking - sync
@router.post("/v1/chunk/hybrid/file")
async def chunk_hybrid_file(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hybrid chunking of a document (sync)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_request(files, "/v1/chunk/hybrid/file", api_key)


# Hybrid chunking - async
@router.post("/v1/chunk/hybrid/file/async")
async def chunk_hybrid_file_async(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hybrid chunking of a document (async)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_request(files, "/v1/chunk/hybrid/file/async", api_key, is_async=True)


# =============================================================================
# SOURCE CHUNK ENDPOINTS
# =============================================================================

# Hierarchical source - sync
@router.post("/v1/chunk/hierarchical/source")
async def chunk_hierarchical_source(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hierarchical chunking from URL source (sync)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_source_request(request, "/v1/chunk/hierarchical/source", api_key)


# Hierarchical source - async
@router.post("/v1/chunk/hierarchical/source/async")
async def chunk_hierarchical_source_async(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hierarchical chunking from URL source (async)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_source_request(request, "/v1/chunk/hierarchical/source/async", api_key, is_async=True)


# Hybrid source - sync
@router.post("/v1/chunk/hybrid/source")
async def chunk_hybrid_source(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hybrid chunking from URL source (sync)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_source_request(request, "/v1/chunk/hybrid/source", api_key)


# Hybrid source - async
@router.post("/v1/chunk/hybrid/source/async")
async def chunk_hybrid_source_async(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Hybrid chunking from URL source (async)."""
    api_key = await _verify_api_key(credentials)
    return await _proxy_chunk_source_request(request, "/v1/chunk/hybrid/source/async", api_key, is_async=True)
