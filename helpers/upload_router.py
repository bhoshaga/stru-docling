"""
Upload and source conversion endpoints.

Handles file uploads and conversion from uploaded files or URLs.
"""
import asyncio
import hashlib
import logging
import os
import time
import uuid
from io import BytesIO
from typing import Dict, Optional

import httpx
from fastapi import APIRouter, Depends, File, HTTPException, Request, UploadFile
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# Router
router = APIRouter(tags=["upload"])

# Upload storage config (matches result_storage default ./data structure)
UPLOADS_DIR = os.environ.get("UPLOADS_DIR", "./data/uploads")
os.makedirs(UPLOADS_DIR, exist_ok=True)

# Uploaded files tracking: {file_id: {"api_key": str, "filepath": str, "filename": str, "uploaded_at": float, "hash": str}}
uploaded_files: Dict[str, dict] = {}

# Hash to file_id mapping for deduplication: {sha256_hash: file_id}
hash_to_file_id: Dict[str, str] = {}

# Security
security = HTTPBearer()


def init_router(
    verify_api_key_func,
    get_workers_for_api_key_func,
    check_file_limits_func,
    get_worker_id_func,
    workers_dict,
    load_balancer_obj,
    job_manager_obj,
    get_page_count_func,
    split_pdf_for_workers_func,
    submit_to_worker_func,
    poll_worker_status_func,
    get_worker_result_func,
    merge_document_results_func,
):
    """
    Initialize the router with dependencies from the main controller.

    This avoids circular imports by injecting dependencies at runtime.
    """
    global _verify_api_key, _get_workers_for_api_key, _check_file_limits
    global _get_worker_id, _workers, _load_balancer, _job_manager
    global _get_page_count, _split_pdf_for_workers, _submit_to_worker
    global _poll_worker_status, _get_worker_result, _merge_document_results

    _verify_api_key = verify_api_key_func
    _get_workers_for_api_key = get_workers_for_api_key_func
    _check_file_limits = check_file_limits_func
    _get_worker_id = get_worker_id_func
    _workers = workers_dict
    _load_balancer = load_balancer_obj
    _job_manager = job_manager_obj
    _get_page_count = get_page_count_func
    _split_pdf_for_workers = split_pdf_for_workers_func
    _submit_to_worker = submit_to_worker_func
    _poll_worker_status = poll_worker_status_func
    _get_worker_result = get_worker_result_func
    _merge_document_results = merge_document_results_func


# =============================================================================
# UPLOAD ENDPOINT
# =============================================================================

@router.post("/v1/upload")
async def upload_file(
    files: UploadFile = File(...),
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Upload a file for later conversion.

    Useful for large files that exceed HTTP body limits.
    Returns a file_id that can be used with /v1/convert/source endpoints.

    Security: Files are stored with UUID names, not original filenames.
    """
    api_key = await _verify_api_key(credentials)

    file_bytes = await files.read()
    original_filename = files.filename or "document.pdf"
    is_pdf = original_filename.lower().endswith(".pdf")
    file_size = len(file_bytes)

    # Calculate SHA256 hash for deduplication
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    logger.info(f"[UPLOAD] Received file: {original_filename}, size={file_size} bytes, hash={file_hash[:16]}..., api_key={api_key}")

    # Check if file already uploaded (same hash)
    if file_hash in hash_to_file_id:
        existing_file_id = hash_to_file_id[file_hash]
        existing_info = uploaded_files.get(existing_file_id)
        if existing_info and os.path.exists(existing_info["filepath"]):
            logger.info(f"[UPLOAD] Duplicate detected: hash={file_hash[:16]}... already exists as file_id={existing_file_id}")
            return {
                "file_id": existing_file_id,
                "filename": original_filename,
                "size_bytes": file_size,
                "duplicate": True,
                "message": "File already uploaded. Returning existing file_id.",
            }
        else:
            # Hash exists but file is gone - clean up stale mapping
            del hash_to_file_id[file_hash]
            if existing_file_id in uploaded_files:
                del uploaded_files[existing_file_id]

    # Check tier limits
    limit_error = _check_file_limits(file_bytes, api_key, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Generate UUID-based filename for security (don't use original filename)
    file_id = str(uuid.uuid4())
    # Keep extension for content-type detection, but use UUID as filename
    ext = os.path.splitext(original_filename)[1].lower() or ".bin"
    # Sanitize extension (only allow known safe extensions)
    safe_extensions = {".pdf", ".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".xls", ".html", ".txt", ".md", ".png", ".jpg", ".jpeg"}
    if ext not in safe_extensions:
        ext = ".bin"

    filepath = os.path.join(UPLOADS_DIR, f"{file_id}{ext}")

    with open(filepath, "wb") as f:
        f.write(file_bytes)

    # Track the upload (store original filename for reference, but file is saved with UUID)
    uploaded_files[file_id] = {
        "api_key": api_key,
        "filepath": filepath,
        "filename": original_filename,  # Original name for display/logging only
        "uploaded_at": time.time(),
        "size_bytes": file_size,
        "hash": file_hash,
    }

    # Register hash mapping for deduplication
    hash_to_file_id[file_hash] = file_id

    logger.info(f"[UPLOAD] Saved file as {file_id}{ext} (original: {original_filename}, hash={file_hash[:16]}...)")

    return {
        "file_id": file_id,
        "filename": original_filename,
        "size_bytes": file_size,
        "message": "File uploaded successfully. Use file_id with /v1/convert/source endpoints.",
    }


# =============================================================================
# CONVERT SOURCE ENDPOINTS
# =============================================================================

@router.post("/v1/convert/source/async")
async def convert_source_async(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Convert a document from source (file_id from upload or URL).

    Request body should be JSON with:
    - file_id: ID returned from /v1/upload
    - OR source: URL to fetch document from
    - to_formats: Output format (default: "json")
    - page_range: Optional [start, end] for PDF pages
    """
    api_key = await _verify_api_key(credentials)

    body = await request.json()
    file_id = body.get("file_id")
    source_url = body.get("source")
    to_formats = body.get("to_formats", "json")
    page_range = body.get("page_range")

    if not file_id and not source_url:
        raise HTTPException(status_code=400, detail="Must provide either file_id or source URL")

    if file_id:
        # Convert from uploaded file
        if file_id not in uploaded_files:
            raise HTTPException(status_code=404, detail="File not found. Upload first with /v1/upload")

        upload_info = uploaded_files[file_id]
        if upload_info["api_key"] != api_key:
            raise HTTPException(status_code=403, detail="File belongs to different API key")

        # Read file from disk
        filepath = upload_info["filepath"]
        if not os.path.exists(filepath):
            raise HTTPException(status_code=404, detail="File no longer exists on disk")

        with open(filepath, "rb") as f:
            file_bytes = f.read()

        filename = upload_info["filename"]
        is_pdf = filename.lower().endswith(".pdf")

        logger.info(f"[SOURCE] Converting uploaded file {file_id}: {filename}")

    else:
        # Proxy to worker for URL source
        logger.info(f"[SOURCE] Proxying URL source to worker: {source_url}")

        # Get available workers
        allowed_ports = _get_workers_for_api_key(api_key)
        available_workers = [
            p for p in allowed_ports
            if _get_worker_id(p) in _workers and _workers[_get_worker_id(p)].get("state") == "ready"
        ]

        if not available_workers:
            raise HTTPException(status_code=503, detail="No workers available")

        worker_port = _load_balancer.select_single_worker(api_key)
        if not worker_port:
            worker_port = available_workers[0]

        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(
                f"http://127.0.0.1:{worker_port}/v1/convert/source/async",
                json=body,
            )

            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)

            result = resp.json()

            # Import chunk_tasks from controller to track for poll/result
            from controller import chunk_tasks
            if "task_id" in result:
                chunk_tasks[result["task_id"]] = worker_port

            return result

    # For file_id: Process like a regular file conversion
    # Get available workers
    allowed_ports = _get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if _get_worker_id(p) in _workers and _workers[_get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # For non-PDF or single worker, route to single worker
    if not is_pdf:
        coolest_port = _load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        job = _job_manager.create_job(filename=filename, total_pages=1, user_page_range=None)
        _job_manager.start_job(job.job_id)

        sub_job = _job_manager.add_sub_job(
            job_id=job.job_id,
            worker_port=coolest_port,
            original_pages=(1, 1),
        )

        task_id, error = await _submit_to_worker(coolest_port, file_bytes, filename, to_formats)
        if error:
            _job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        _job_manager.register_worker_task(sub_job.sub_job_id, task_id)

        return {
            "task_id": job.job_id,
            "status": "pending",
            "total_pages": 1,
            "workers_used": 1,
        }

    # PDF: Split across workers
    try:
        total_pages = _get_page_count(file_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    if user_range:
        start_page, end_page = user_range
        start_page = max(1, min(start_page, total_pages))
        end_page = max(start_page, min(end_page, total_pages))
        pages_to_process = end_page - start_page + 1
    else:
        start_page, end_page = 1, total_pages
        pages_to_process = total_pages

    job = _job_manager.create_job(
        filename=filename,
        total_pages=pages_to_process,
        user_page_range=user_range,
    )
    logger.info(f"[JOB {job.job_id}] Created from file_id: {filename}, {pages_to_process} pages, {len(available_workers)} workers available")

    try:
        chunks = _split_pdf_for_workers(file_bytes, len(available_workers), user_range)
        # Log detailed split info
        logger.info(f"[JOB {job.job_id}] SPLIT: {len(chunks)} chunks across {len(chunks)} workers")
        for i, (chunk_bytes, pr) in enumerate(chunks):
            worker_port = available_workers[i]
            chunk_mb = len(chunk_bytes) / (1024 * 1024)
            pages = pr[1] - pr[0] + 1
            logger.info(f"[JOB {job.job_id}]   Chunk {i+1}: pages {pr[0]}-{pr[1]} ({pages} pages, {chunk_mb:.1f}MB) -> worker {worker_port}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error splitting PDF: {e}")

    _job_manager.start_job(job.job_id)

    async def submit_chunk(chunk_data, port):
        chunk_bytes, original_pages = chunk_data
        sub_job = _job_manager.add_sub_job(
            job_id=job.job_id,
            worker_port=port,
            original_pages=original_pages,
        )
        task_id, error = await _submit_to_worker(port, chunk_bytes, filename, to_formats)
        if error:
            _job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
            return False
        _job_manager.register_worker_task(sub_job.sub_job_id, task_id)
        return True

    tasks = [submit_chunk(chunk, port) for chunk, port in zip(chunks, available_workers)]
    await asyncio.gather(*tasks)

    return {
        "task_id": job.job_id,
        "status": "pending",
        "total_pages": pages_to_process,
        "workers_used": len(chunks),
    }


@router.post("/v1/convert/source")
async def convert_source_sync(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """
    Synchronous version of /v1/convert/source/async.
    Blocks until conversion is complete and returns the result.
    """
    api_key = await _verify_api_key(credentials)

    body = await request.json()
    file_id = body.get("file_id")
    source_url = body.get("source")
    to_formats = body.get("to_formats", "json")
    page_range = body.get("page_range")

    if not file_id and not source_url:
        raise HTTPException(status_code=400, detail="Must provide either file_id or source URL")

    if source_url:
        # Proxy to worker for URL source
        allowed_ports = _get_workers_for_api_key(api_key)
        available_workers = [
            p for p in allowed_ports
            if _get_worker_id(p) in _workers and _workers[_get_worker_id(p)].get("state") == "ready"
        ]

        if not available_workers:
            raise HTTPException(status_code=503, detail="No workers available")

        worker_port = _load_balancer.select_single_worker(api_key)
        if not worker_port:
            worker_port = available_workers[0]

        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.post(
                f"http://127.0.0.1:{worker_port}/v1/convert/source",
                json=body,
            )

            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)

            return resp.json()

    # For file_id: Get file and process
    if file_id not in uploaded_files:
        raise HTTPException(status_code=404, detail="File not found")

    upload_info = uploaded_files[file_id]
    if upload_info["api_key"] != api_key:
        raise HTTPException(status_code=403, detail="File belongs to different API key")

    filepath = upload_info["filepath"]
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail="File no longer exists on disk")

    with open(filepath, "rb") as f:
        file_bytes = f.read()

    filename = upload_info["filename"]
    is_pdf = filename.lower().endswith(".pdf")

    # Get available workers
    allowed_ports = _get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if _get_worker_id(p) in _workers and _workers[_get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # Non-PDF: single worker
    if not is_pdf:
        coolest_port = _load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        task_id, error = await _submit_to_worker(coolest_port, file_bytes, filename, to_formats)
        if error:
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        while True:
            status, _ = await _poll_worker_status(coolest_port, task_id)
            if status == "success":
                result, error = await _get_worker_result(coolest_port, task_id)
                if error:
                    raise HTTPException(status_code=500, detail=f"Failed to get result: {error}")
                return result
            elif status == "failure":
                raise HTTPException(status_code=500, detail="Worker task failed")
            await asyncio.sleep(0.5)

    # PDF: Split and wait for all
    try:
        total_pages = _get_page_count(file_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    try:
        chunks = _split_pdf_for_workers(file_bytes, len(available_workers), user_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error splitting PDF: {e}")

    # Submit all chunks
    task_info = []

    async def submit_chunk(chunk_data, port):
        chunk_bytes, original_pages = chunk_data
        task_id, error = await _submit_to_worker(port, chunk_bytes, filename, to_formats)
        if error:
            return None
        return (port, task_id, original_pages)

    tasks = [submit_chunk(chunk, port) for chunk, port in zip(chunks, available_workers)]
    results = await asyncio.gather(*tasks)
    task_info = [r for r in results if r is not None]

    if not task_info:
        raise HTTPException(status_code=500, detail="Failed to submit to any worker")

    # Poll until all complete
    max_wait = 600
    start_time = time.time()
    completed_results = {}

    while len(completed_results) < len(task_info):
        if time.time() - start_time > max_wait:
            raise HTTPException(status_code=504, detail=f"Conversion timed out after {max_wait}s")

        for port, task_id, page_range_chunk in task_info:
            if task_id in completed_results:
                continue

            status, _ = await _poll_worker_status(port, task_id)

            if status == "success":
                result, error = await _get_worker_result(port, task_id)
                if result:
                    completed_results[task_id] = {"pages": page_range_chunk, "result": result}
                else:
                    completed_results[task_id] = {"pages": page_range_chunk, "result": {"status": "failure", "errors": [{"message": error}]}}
            elif status == "failure":
                completed_results[task_id] = {"pages": page_range_chunk, "result": {"status": "failure", "errors": [{"message": "Worker reported failure"}]}}

        if len(completed_results) < len(task_info):
            await asyncio.sleep(0.5)

    # Merge results
    sub_results = list(completed_results.values())
    return _merge_document_results(sub_results, filename)


def cleanup_old_uploads(max_age_seconds: int = 3600) -> int:
    """Remove uploaded files older than max_age_seconds."""
    now = time.time()
    to_remove = []

    for file_id, info in uploaded_files.items():
        age = now - info["uploaded_at"]
        if age > max_age_seconds:
            to_remove.append(file_id)

    for file_id in to_remove:
        info = uploaded_files.pop(file_id, None)
        if info:
            # Clean up hash mapping
            file_hash = info.get("hash")
            if file_hash and file_hash in hash_to_file_id:
                del hash_to_file_id[file_hash]

            # Delete file from disk
            if os.path.exists(info["filepath"]):
                try:
                    os.remove(info["filepath"])
                    logger.info(f"[UPLOAD] Cleaned up old file: {file_id} (hash={file_hash[:16] if file_hash else 'N/A'}...)")
                except Exception as e:
                    logger.warning(f"[UPLOAD] Failed to delete file {file_id}: {e}")

    return len(to_remove)
