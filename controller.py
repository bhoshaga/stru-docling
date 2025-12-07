#!/usr/bin/env python3
"""
Docling Worker Controller v2

Manages docling-serve workers with load balancing and page splitting.
No Redis dependency - uses in-memory job tracking with disk persistence.

Key features:
- Page splitting across workers for parallel processing
- Automatic load balancing
- Result persistence to disk
- Worker health monitoring and auto-restart
"""

import asyncio
from datetime import datetime, timezone
import logging
import os
import signal
import subprocess
import time
import uuid
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Dict, List, Optional, Tuple


def epoch_to_utc(epoch: Optional[float]) -> Optional[str]:
    """Convert epoch timestamp to UTC ISO format string."""
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

import httpx
import psutil
from fastapi import FastAPI, HTTPException, Request, Response, Depends, UploadFile, File, Form
from fastapi.responses import RedirectResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# =============================================================================
# LOGGING SETUP
# =============================================================================

LOG_FILE = os.environ.get("LOG_FILE", "./server.log")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

# Create log directory if needed (only if there's a directory component)
log_dir = os.path.dirname(LOG_FILE)
if log_dir:
    os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

from helpers import (
    get_page_count,
    split_pdf_for_workers,
    job_manager,
    JobStatus,
    load_balancer,
    result_storage,
    upload_router,
    init_upload_router,
    cleanup_old_uploads,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Watchdog settings
MAX_PAGES_BEFORE_RESTART = int(os.environ.get("MAX_PAGES_BEFORE_RESTART", "10"))
MAX_MEMORY_MB = int(os.environ.get("MAX_MEMORY_MB", "12000"))
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "600"))
IDLE_RESTART_SEC = int(os.environ.get("IDLE_RESTART_SEC", "3600"))
JOB_TIMEOUT_SEC = int(os.environ.get("JOB_TIMEOUT_SEC", "600"))  # 10 minutes

# Worker settings
WORKER_HOST = "127.0.0.1"
NUM_THREADS_PER_WORKER = int(os.environ.get("DOCLING_SERVE_ENG_LOC_NUM_WORKERS", "2"))
MIN_PORT = 5001
MAX_PORT = 5010
NUM_WORKERS = MAX_PORT - MIN_PORT + 1

# API authentication - Two tiers with limits
# "windowseat" = dedicated tier (8 workers: ports 5001-5008)
# "middleseat" = shared/free tier (2 workers: ports 5009-5010)
API_KEYS = {
    "windowseat": {
        "tier": "dedicated",
        "workers": list(range(5001, 5009)),  # 8 workers
        "max_file_mb": 200,  # Max file size in MB
        "max_pages": 400,    # Max pages per PDF
    },
    "middleseat": {
        "tier": "shared",
        "workers": list(range(5009, 5011)),  # 2 workers
        "max_file_mb": 20,   # Max file size in MB
        "max_pages": 20,     # Max pages per PDF
    },
}

# =============================================================================
# LIFESPAN
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown."""
    # === STARTUP ===
    app.state.start_time = time.time()
    logger.info("=" * 60)
    logger.info("[CONTROLLER] STARTING - Controller v2.0.0")
    logger.info("=" * 60)

    # Load saved stats if available
    saved_stats = result_storage.load_stats()
    if saved_stats:
        job_manager.stats.update(saved_stats)
        logger.info(f"[CONTROLLER] Loaded job stats: {saved_stats}")

    # Load saved worker stats if available
    saved_worker_stats = result_storage.load_worker_stats()
    if saved_worker_stats:
        logger.info(f"[CONTROLLER] Loaded worker stats from disk: {saved_worker_stats}")

    # Give workers time to start (started by startup.sh)
    await asyncio.sleep(2)

    # Find existing workers
    for proc in psutil.process_iter(['pid', 'cmdline']):
        try:
            cmdline = proc.info['cmdline']
            if cmdline and 'docling-serve' in ' '.join(cmdline):
                for i, arg in enumerate(cmdline):
                    if arg == '--port' and i + 1 < len(cmdline):
                        try:
                            port = int(cmdline[i + 1])
                            if MIN_PORT <= port <= MAX_PORT:
                                worker_id = get_worker_id(port)
                                # Restore stats from saved if available
                                restored = saved_worker_stats.get(str(port), {}) if saved_worker_stats else {}
                                workers[worker_id] = {
                                    "pid": proc.info['pid'],
                                    "port": port,
                                    "state": "ready",
                                    # Current life stats (reset on worker restart)
                                    "current_life_pages": restored.get("current_life_pages", 0),
                                    # Lifetime stats (never reset)
                                    "lifetime_pages": restored.get("lifetime_pages", 0),
                                    "restart_count": restored.get("restart_count", 0),
                                    "last_restart_at": restored.get("last_restart_at"),
                                    "last_restart_reason": restored.get("last_restart_reason"),
                                    # Activity tracking
                                    "last_activity": time.time(),
                                    "current_job_id": None,
                                    "started_at": restored.get("started_at", time.time()),
                                    "memory_mb": 0,
                                }
                                load_balancer.register_worker(port, workers[worker_id])
                                logger.info(f"[CONTROLLER] Found worker {worker_id} on port {port}, pid={proc.info['pid']}")
                                logger.info(f"[CONTROLLER]   Lifetime pages: {workers[worker_id]['lifetime_pages']}, Current life: {workers[worker_id]['current_life_pages']}, Restarts: {workers[worker_id]['restart_count']}")
                        except ValueError:
                            pass
                        break
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    logger.info(f"[CONTROLLER] Startup complete. Found {len(workers)} workers: {sorted(workers.keys())}")

    # Configure API key pools in load balancer for tier isolation
    for api_key, config in API_KEYS.items():
        allowed_ports = config.get("workers", [])
        load_balancer.assign_api_key_pool(api_key, allowed_ports)
        logger.info(f"[CONTROLLER] Assigned API key '{api_key}' to ports: {allowed_ports}")

    # Initialize upload router with dependencies
    init_upload_router(
        verify_api_key_func=verify_api_key,
        get_workers_for_api_key_func=get_workers_for_api_key,
        check_file_limits_func=check_file_limits,
        get_worker_id_func=get_worker_id,
        workers_dict=workers,
        load_balancer_obj=load_balancer,
        job_manager_obj=job_manager,
        get_page_count_func=get_page_count,
        split_pdf_for_workers_func=split_pdf_for_workers,
        submit_to_worker_func=submit_to_worker,
        poll_worker_status_func=poll_worker_status,
        get_worker_result_func=get_worker_result,
        merge_document_results_func=_merge_document_results,
    )

    # Start background tasks
    background_tasks.append(asyncio.create_task(watchdog()))
    background_tasks.append(asyncio.create_task(cleanup_task()))

    yield  # Server runs here

    # === SHUTDOWN ===
    logger.info("=" * 60)
    logger.info("[CONTROLLER] SHUTTING DOWN")
    logger.info("=" * 60)

    # Save final job stats
    stats = job_manager.get_stats()
    result_storage.save_stats(stats)
    logger.info(f"[CONTROLLER] Saved job stats: {stats}")

    # Save worker stats (all tracking fields)
    worker_stats = {}
    for worker_id, worker in workers.items():
        worker_stats[str(worker["port"])] = {
            "current_life_pages": worker.get("current_life_pages", 0),
            "lifetime_pages": worker.get("lifetime_pages", 0),
            "restart_count": worker.get("restart_count", 0),
            "last_restart_at": worker.get("last_restart_at"),
            "last_restart_reason": worker.get("last_restart_reason"),
            "started_at": worker.get("started_at"),
            "last_activity": worker.get("last_activity"),
        }
    result_storage.save_worker_stats(worker_stats)
    logger.info(f"[CONTROLLER] Saved worker stats: {worker_stats}")

    # Cancel background tasks
    for task in background_tasks:
        task.cancel()

    logger.info("[CONTROLLER] Shutdown complete")


# =============================================================================
# GLOBALS
# =============================================================================

app = FastAPI(title="Docling Controller", version="2.0.0", lifespan=lifespan)

# Include upload router
app.include_router(upload_router)

# Round-robin index for load balancing proxy requests
_rr_index = 0
security = HTTPBearer()

# Worker tracking: {worker_id: {"pid": int, "port": int, ...}}
workers: Dict[str, dict] = {}

# Chunk task tracking: {task_id: worker_port} - for proxying poll/result
chunk_tasks: Dict[str, int] = {}

# Background tasks
background_tasks: List[asyncio.Task] = []

# Thread pool for parallel requests
thread_pool = ThreadPoolExecutor(max_workers=NUM_WORKERS * 2)


# =============================================================================
# AUTHENTICATION
# =============================================================================

async def verify_api_key(credentials: HTTPAuthorizationCredentials = Depends(security)):
    api_key = credentials.credentials
    if api_key not in API_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


def get_workers_for_api_key(api_key: str) -> List[int]:
    """Get the list of worker ports available for an API key."""
    if api_key in API_KEYS:
        return API_KEYS[api_key]["workers"]
    return []  # No workers for invalid key


def get_limits_for_api_key(api_key: str) -> Tuple[int, int]:
    """Get (max_file_mb, max_pages) limits for an API key."""
    if api_key in API_KEYS:
        config = API_KEYS[api_key]
        return config.get("max_file_mb", 200), config.get("max_pages", 400)
    return 20, 20  # Default to shared tier limits


def check_file_limits(file_bytes: bytes, api_key: str, is_pdf: bool = True) -> Optional[str]:
    """
    Check if file meets tier limits. Returns error message if exceeded, None if OK.
    """
    max_file_mb, max_pages = get_limits_for_api_key(api_key)

    # Check file size
    file_mb = len(file_bytes) / (1024 * 1024)
    if file_mb > max_file_mb:
        return f"File size {file_mb:.1f}MB exceeds limit of {max_file_mb}MB for your tier"

    # Check page count (PDF only)
    if is_pdf:
        try:
            page_count = get_page_count(file_bytes)
            if page_count > max_pages:
                return f"Page count {page_count} exceeds limit of {max_pages} pages for your tier"
        except Exception:
            pass  # If we can't count pages, let it through - worker will validate

    return None  # All checks passed


# =============================================================================
# WORKER MANAGEMENT
# =============================================================================

def get_worker_id(port: int) -> str:
    """Get worker ID from port number."""
    return f"docling_{port - MIN_PORT + 1}"


def get_port_from_worker_id(worker_id: str) -> int:
    """Get port from worker ID."""
    num = int(worker_id.split("_")[1])
    return MIN_PORT + num - 1


def get_worker_cmd(port: int) -> list[str]:
    """Get command to start a worker."""
    return ["docling-serve", "run", "--host", "0.0.0.0", "--port", str(port)]


def start_worker_process(port: int) -> int:
    """Start a docling-serve worker process. Returns PID."""
    env = os.environ.copy()
    env["DOCLING_SERVE_ENG_LOC_NUM_WORKERS"] = str(NUM_THREADS_PER_WORKER)

    proc = subprocess.Popen(
        get_worker_cmd(port),
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True
    )
    return proc.pid


def kill_worker_process(pid: int) -> bool:
    """Kill a worker process by PID.

    Uses os.kill() instead of os.killpg() to avoid killing other workers
    that might share the same process group (e.g., when started from same shell).
    """
    try:
        logger.info(f"Killing worker process {pid}")
        os.kill(pid, signal.SIGTERM)
        time.sleep(0.5)
        if psutil.pid_exists(pid):
            logger.info(f"Process {pid} still alive, sending SIGKILL")
            os.kill(pid, signal.SIGKILL)
        return True
    except (ProcessLookupError, PermissionError) as e:
        logger.warning(f"Failed to kill process {pid}: {e}")
        return False


async def init_worker(port: int) -> str:
    """Initialize a worker and wait for it to be ready."""
    worker_id = get_worker_id(port)
    pid = start_worker_process(port)

    workers[worker_id] = {
        "pid": pid,
        "port": port,
        "state": "starting",
        # Current life stats (reset on worker restart)
        "current_life_pages": 0,
        # Lifetime stats (never reset)
        "lifetime_pages": 0,
        "restart_count": 0,
        "last_restart_at": None,
        "last_restart_reason": None,
        # Activity tracking
        "last_activity": time.time(),
        "current_job_id": None,
        "started_at": time.time(),
        "memory_mb": 0,
    }

    # Wait for worker to be ready (max 60s)
    for _ in range(60):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"http://{WORKER_HOST}:{port}/health")
                if resp.status_code == 200:
                    workers[worker_id]["state"] = "ready"
                    break
        except Exception:
            pass
        await asyncio.sleep(1)

    # Register with load balancer
    load_balancer.register_worker(port, workers[worker_id])

    return worker_id


async def restart_worker(worker_id: str, reason: str = "manual"):
    """Restart a worker, saving pending results first."""
    if worker_id not in workers:
        return False

    worker = workers[worker_id]
    port = worker["port"]
    old_pid = worker["pid"]
    pages_before = worker["pages_processed"]

    logger.info("=" * 60)
    logger.info(f"[WORKER RESTART] {worker_id} (port {port}) - STARTING")
    logger.info(f"[WORKER RESTART] Reason: {reason}")
    logger.info(f"[WORKER RESTART] Old PID: {old_pid}, Pages processed: {pages_before}")
    logger.info("=" * 60)

    # Mark as restarting to block new task assignments
    workers[worker_id]["state"] = "restarting"
    load_balancer.update_worker(port, workers[worker_id])
    logger.info(f"[WORKER RESTART] {worker_id}: Saving pending results before restart...")

    # Poll and save all pending sub-jobs before restart
    pending_sub_jobs = job_manager.get_pending_sub_jobs_for_worker(port)
    logger.info(f"[RESTART] {worker_id}: got {len(pending_sub_jobs)} pending sub-jobs to poll before restart")
    for job_id, sub_job in pending_sub_jobs:
        logger.info(f"[RESTART] {worker_id}: checking sub_job {sub_job.sub_job_id[:8]}, task_id={sub_job.worker_task_id}")
        if not sub_job.worker_task_id:
            logger.info(f"[RESTART] {worker_id}: sub_job has no task_id, skipping")
            continue
        try:
            # Poll worker for status
            logger.info(f"[RESTART] {worker_id}: polling task {sub_job.worker_task_id[:8]} on port {port}...")
            status, _ = await poll_worker_status(port, sub_job.worker_task_id)
            logger.info(f"[RESTART] {worker_id}: task {sub_job.worker_task_id[:8]} status={status}")
            if status == "success":
                # Fetch result and complete sub-job
                result, error = await get_worker_result(port, sub_job.worker_task_id)
                if result:
                    job_manager.complete_sub_job(sub_job.sub_job_id, result=result)
                    logger.info(f"Saved result for sub-job {sub_job.sub_job_id} before restart")
                else:
                    job_manager.complete_sub_job(sub_job.sub_job_id, error=error or "Failed to fetch result")
            elif status == "failure":
                job_manager.complete_sub_job(sub_job.sub_job_id, error="Worker task failed")
                logger.info(f"[RESTART] {worker_id}: task failed for sub_job {sub_job.sub_job_id[:8]}")
            # If still processing, we'll lose this result - log warning
            elif status == "started":
                logger.warning(f"Sub-job {sub_job.sub_job_id} still processing, may lose result on restart")
            else:
                logger.warning(f"[RESTART] {worker_id}: unexpected status '{status}' for task {sub_job.worker_task_id[:8]}")
        except Exception as e:
            logger.warning(f"Could not save pending result for {job_id}: {e}")

    # Now check if any parent jobs completed and save to disk
    for job_id, _ in pending_sub_jobs:
        job = job_manager.get_job(job_id)
        if job and job.status == JobStatus.COMPLETED and not result_storage.result_exists(job_id):
            merged = _build_merged_result(job)
            result_storage.save_result(job_id, merged)
            logger.info(f"Job {job_id} saved to disk before worker restart")

    # Kill old process
    if old_pid and psutil.pid_exists(old_pid):
        kill_worker_process(old_pid)

    await asyncio.sleep(1)  # Wait for port release

    # Start new process
    new_pid = start_worker_process(port)

    # Preserve lifetime stats, reset current life stats
    old_lifetime_pages = worker.get("lifetime_pages", 0)
    old_restart_count = worker.get("restart_count", 0)

    workers[worker_id] = {
        "pid": new_pid,
        "port": port,
        "state": "starting",
        # Current life resets
        "current_life_pages": 0,
        # Lifetime stats preserved and updated
        "lifetime_pages": old_lifetime_pages,  # Preserved (pages added during job completion)
        "restart_count": old_restart_count + 1,
        "last_restart_at": time.time(),
        "last_restart_reason": reason,
        # Activity tracking
        "last_activity": time.time(),
        "current_job_id": None,
        "started_at": time.time(),
        "memory_mb": 0,
    }

    # Wait for worker to be ready (max 60s)
    for _ in range(60):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                resp = await client.get(f"http://{WORKER_HOST}:{port}/health")
                if resp.status_code == 200:
                    workers[worker_id]["state"] = "ready"
                    break
        except Exception:
            pass
        await asyncio.sleep(1)

    # Update load balancer
    load_balancer.update_worker(port, workers[worker_id])

    logger.info("=" * 60)
    logger.info(f"[WORKER RESTART] {worker_id} (port {port}) - COMPLETE")
    logger.info(f"[WORKER RESTART] New PID: {new_pid}")
    logger.info(f"[WORKER RESTART] Lifetime pages: {old_lifetime_pages}, Current life pages: 0 (reset)")
    logger.info(f"[WORKER RESTART] Total restarts: {old_restart_count + 1}")
    logger.info("=" * 60)
    return True


async def remove_worker(worker_id: str) -> bool:
    """Remove a worker completely."""
    if worker_id not in workers:
        return False

    worker = workers[worker_id]
    port = worker["port"]

    if worker["pid"] and psutil.pid_exists(worker["pid"]):
        kill_worker_process(worker["pid"])

    del workers[worker_id]
    load_balancer.unregister_worker(port)

    return True


def update_worker_state(worker_id: str, state: str):
    """Update worker state."""
    if worker_id in workers:
        workers[worker_id]["state"] = state
        workers[worker_id]["last_activity"] = time.time()
        load_balancer.update_worker(workers[worker_id]["port"], workers[worker_id])


# =============================================================================
# WORKER REQUEST HELPERS
# =============================================================================

async def submit_to_worker(
    port: int,
    pdf_bytes: bytes,
    filename: str = "document.pdf",
    to_formats: str = "json",
) -> Tuple[Optional[str], Optional[str]]:
    """
    Submit a PDF chunk to a worker.

    Returns:
        (task_id, error) - task_id if successful, error message if failed
    """
    url = f"http://{WORKER_HOST}:{port}/v1/convert/file/async"
    logger.info(f"Submitting to worker {port}: {len(pdf_bytes)} bytes, format={to_formats}")

    # Update worker state and activity when job is submitted
    worker_id = get_worker_id(port)
    if worker_id in workers:
        workers[worker_id]["last_activity"] = time.time()
        workers[worker_id]["state"] = "processing"
        load_balancer.update_worker(port, workers[worker_id])

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SEC) as client:
            # Must wrap bytes in BytesIO for httpx async file uploads
            files = {"files": (filename, BytesIO(pdf_bytes), "application/pdf")}
            data = {"to_formats": to_formats}

            response = await client.post(url, files=files, data=data)

            if response.status_code == 200:
                result = response.json()
                task_id = result.get("task_id")
                logger.info(f"Worker {port} accepted task: {task_id}")
                # Update activity again on successful submission
                if worker_id in workers:
                    workers[worker_id]["last_activity"] = time.time()
                return task_id, None
            else:
                error = f"Worker returned {response.status_code}: {response.text[:200]}"
                logger.error(f"Worker {port} error: {error}")
                return None, error

    except httpx.TimeoutException:
        error = f"Timeout submitting to worker on port {port}"
        logger.error(error)
        return None, error
    except httpx.ConnectError:
        error = f"Cannot connect to worker on port {port}"
        logger.error(error)
        return None, error
    except Exception as e:
        logger.error(f"Worker {port} exception: {e}")
        return None, str(e)


async def poll_worker_status(port: int, task_id: str) -> Tuple[str, Optional[dict]]:
    """
    Poll a worker for task status.

    Returns:
        (status, result_if_complete)
    """
    url = f"http://{WORKER_HOST}:{port}/v1/status/poll/{task_id}"

    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                data = response.json()
                return data.get("task_status", "unknown"), data
            else:
                return "error", {"error": f"Status {response.status_code}"}

    except Exception as e:
        return "error", {"error": str(e)}


async def get_worker_result(port: int, task_id: str) -> Tuple[Optional[dict], Optional[str]]:
    """
    Get result from a worker.

    Returns:
        (result, error)
    """
    url = f"http://{WORKER_HOST}:{port}/v1/result/{task_id}"

    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.get(url)
            if response.status_code == 200:
                # Update worker activity on successful result retrieval
                worker_id = get_worker_id(port)
                if worker_id in workers:
                    workers[worker_id]["last_activity"] = time.time()
                return response.json(), None
            else:
                return None, f"Worker returned {response.status_code}"

    except Exception as e:
        return None, str(e)


# =============================================================================
# DOCLING API ENDPOINTS (Load Balanced)
# =============================================================================

@app.post("/v1/convert/file/async")
async def convert_file_async(
    request: Request,
    files: UploadFile = File(...),
    to_formats: str = Form("json"),
    page_range: Optional[List[str]] = Form(None),
    api_key: str = Depends(verify_api_key),
):
    """
    Convert a PDF file with automatic page splitting across workers.

    The PDF is split across available workers for parallel processing.
    Workers are selected based on API key tier:
    - "windowseat" (dedicated): 8 workers (ports 5001-5008)
    - "middleseat" (shared): 2 workers (ports 5009-5010)

    Returns a job_id that can be used to poll for status and get results.
    """
    # Read file bytes
    file_bytes = await files.read()
    filename = files.filename or "document.pdf"
    is_pdf = filename.lower().endswith(".pdf")
    logger.info(f"Received file: {filename}, size={len(file_bytes)} bytes, is_pdf={is_pdf}, api_key={api_key}")

    # Check tier limits
    limit_error = check_file_limits(file_bytes, api_key, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Get available workers for this API key
    allowed_ports = get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # For non-PDF files, route to single coolest worker
    if not is_pdf:
        # Pick the least busy worker
        # Use load balancer to pick best worker for this api_key
        coolest_port = load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        logger.info(f"Non-PDF file, routing to single worker: {coolest_port}")

        # Create job with single sub-job
        job = job_manager.create_job(filename=filename, total_pages=1, user_page_range=None)
        job_manager.start_job(job.job_id)

        sub_job = job_manager.add_sub_job(
            job_id=job.job_id,
            worker_port=coolest_port,
            original_pages=(1, 1),
        )

        task_id, error = await submit_to_worker(coolest_port, file_bytes, filename, to_formats)
        if error:
            job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        job_manager.register_worker_task(sub_job.sub_job_id, task_id)

        return {
            "task_id": job.job_id,
            "status": "pending",
            "total_pages": 1,
            "workers_used": 1,
        }

    # PDF: Get page count and split across workers
    try:
        total_pages = get_page_count(file_bytes)
        logger.info(f"PDF has {total_pages} pages")
    except Exception as e:
        logger.error(f"Invalid PDF: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    # Parse user page range if provided
    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    # Determine pages to process
    if user_range:
        start_page, end_page = user_range
        start_page = max(1, min(start_page, total_pages))
        end_page = max(start_page, min(end_page, total_pages))
        pages_to_process = end_page - start_page + 1
    else:
        start_page, end_page = 1, total_pages
        pages_to_process = total_pages

    # Create parent job
    job = job_manager.create_job(
        filename=filename,
        total_pages=pages_to_process,
        user_page_range=user_range,
    )
    logger.info(f"[JOB {job.job_id}] Created: {filename}, {pages_to_process} pages, {len(available_workers)} workers available")

    # Split PDF across workers
    try:
        chunks = split_pdf_for_workers(file_bytes, len(available_workers), user_range)
        # Log detailed split info
        logger.info(f"[JOB {job.job_id}] SPLIT: {len(chunks)} chunks across {len(chunks)} workers")
        for i, (chunk_bytes, page_range) in enumerate(chunks):
            worker_port = available_workers[i]
            chunk_mb = len(chunk_bytes) / (1024 * 1024)
            pages = page_range[1] - page_range[0] + 1
            logger.info(f"[JOB {job.job_id}]   Chunk {i+1}: pages {page_range[0]}-{page_range[1]} ({pages} pages, {chunk_mb:.1f}MB) -> worker {worker_port}")
    except Exception as e:
        logger.error(f"Error splitting PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Error splitting PDF: {e}")

    # Create sub-jobs and submit to workers
    job_manager.start_job(job.job_id)

    async def submit_chunk(chunk_data: Tuple[bytes, Tuple[int, int]], port: int):
        chunk_bytes, original_pages = chunk_data

        # Create sub-job
        sub_job = job_manager.add_sub_job(
            job_id=job.job_id,
            worker_port=port,
            original_pages=original_pages,
        )

        # Submit to worker
        task_id, error = await submit_to_worker(port, chunk_bytes, filename, to_formats)

        if error:
            job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
            return False

        # Register worker task
        job_manager.register_worker_task(sub_job.sub_job_id, task_id)
        return True

    # Submit all chunks in parallel
    tasks = []
    for i, (chunk, port) in enumerate(zip(chunks, available_workers)):
        tasks.append(submit_chunk(chunk, port))

    await asyncio.gather(*tasks)

    # Update worker page counts (both current life and lifetime)
    for port in available_workers[:len(chunks)]:
        worker_id = get_worker_id(port)
        if worker_id in workers:
            pages_for_worker = pages_to_process // len(chunks)
            workers[worker_id]["current_life_pages"] = workers[worker_id].get("current_life_pages", 0) + pages_for_worker
            workers[worker_id]["lifetime_pages"] = workers[worker_id].get("lifetime_pages", 0) + pages_for_worker
            load_balancer.update_worker(port, workers[worker_id])

    return {
        "task_id": job.job_id,  # Use parent job_id as task_id for client
        "status": "pending",
        "total_pages": pages_to_process,
        "workers_used": len(chunks),
    }


def _merge_document_results(sub_results: List[dict], filename: str) -> dict:
    """
    Merge document results from multiple workers into a single response.

    Args:
        sub_results: List of (page_range, result) from each worker
        filename: Original filename

    Returns:
        Merged ConvertDocumentResponse matching docling-serve format
    """
    if not sub_results:
        return {
            "document": {"filename": filename},
            "status": "failure",
            "errors": [{"message": "No results to merge"}],
            "processing_time": 0,
            "timings": {},
        }

    # Sort sub_results by page range start
    sorted_results = sorted(sub_results, key=lambda x: x["pages"][0])

    # Initialize merged document
    merged_doc = {
        "filename": filename,
        "md_content": None,
        "json_content": None,
        "html_content": None,
        "text_content": None,
        "doctags_content": None,
    }

    total_time = 0
    all_errors = []
    all_timings = {}
    has_success = False

    # Track content to merge
    md_parts = []
    html_parts = []
    text_parts = []
    doctags_parts = []
    json_contents = []

    for item in sorted_results:
        result = item.get("result", {})
        doc = result.get("document", {})

        # Track status
        if result.get("status") == "success":
            has_success = True

        # Aggregate times and errors
        total_time += result.get("processing_time", 0)
        all_errors.extend(result.get("errors", []))

        # Merge timings
        for k, v in result.get("timings", {}).items():
            if k not in all_timings:
                all_timings[k] = v

        # Collect string content
        if doc.get("md_content"):
            md_parts.append(doc["md_content"])
        if doc.get("html_content"):
            html_parts.append(doc["html_content"])
        if doc.get("text_content"):
            text_parts.append(doc["text_content"])
        if doc.get("doctags_content"):
            doctags_parts.append(doc["doctags_content"])
        if doc.get("json_content"):
            json_contents.append((item["pages"], doc["json_content"]))

    # Merge string content (simple concatenation)
    if md_parts:
        merged_doc["md_content"] = "\n\n".join(md_parts)
    if html_parts:
        merged_doc["html_content"] = "\n".join(html_parts)
    if text_parts:
        merged_doc["text_content"] = "\n\n".join(text_parts)
    if doctags_parts:
        merged_doc["doctags_content"] = "\n".join(doctags_parts)

    # Merge JSON content (more complex)
    if json_contents:
        merged_doc["json_content"] = _merge_json_content(json_contents, filename)

    return {
        "document": merged_doc,
        "status": "success" if has_success else "failure",
        "errors": all_errors,
        "processing_time": total_time,
        "timings": all_timings,
    }


def _merge_json_content(json_contents: List[Tuple[tuple, dict]], filename: str) -> dict:
    """
    Merge json_content from multiple workers.

    Args:
        json_contents: List of ((start_page, end_page), json_content) tuples

    Note: docling-serve returns page numbers relative to each chunk (1, 2, 3...)
    not the original PDF page numbers. We need to renumber them.
    """
    if not json_contents:
        return None

    # Use first chunk as base
    first_range, first_jc = json_contents[0]
    base = first_jc.copy()

    # Lists to concatenate
    list_keys = ["groups", "texts", "pictures", "tables", "key_value_items", "form_items"]

    # Renumber pages from first chunk to original page numbers
    merged_pages = {}
    for rel_page_str, page_data in first_jc.get("pages", {}).items():
        rel_page = int(rel_page_str)
        # Convert relative page (1, 2, ...) to original page number
        original_page = first_range[0] + rel_page - 1
        merged_pages[str(original_page)] = page_data

    # Merge lists and pages from subsequent chunks
    for page_range, jc in json_contents[1:]:
        # Merge list contents
        for key in list_keys:
            if key in base and key in jc:
                base[key] = base.get(key, []) + jc.get(key, [])

        # Merge pages with renumbering
        for rel_page_str, page_data in jc.get("pages", {}).items():
            rel_page = int(rel_page_str)
            # Convert relative page to original page number
            original_page = page_range[0] + rel_page - 1
            merged_pages[str(original_page)] = page_data

    base["pages"] = merged_pages
    return base


@app.post("/v1/convert/file")
async def convert_file_sync(
    request: Request,
    files: UploadFile = File(...),
    to_formats: str = Form("json"),
    page_range: Optional[List[str]] = Form(None),
    api_key: str = Depends(verify_api_key),
):
    """
    Synchronous PDF conversion with automatic page splitting across workers.

    Same as /v1/convert/file/async but blocks until complete and returns
    the merged result directly.
    """
    # Read file bytes
    file_bytes = await files.read()
    filename = files.filename or "document.pdf"
    is_pdf = filename.lower().endswith(".pdf")
    logger.info(f"[SYNC] Received file: {filename}, size={len(file_bytes)} bytes, is_pdf={is_pdf}")

    # Check tier limits
    limit_error = check_file_limits(file_bytes, api_key, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Get available workers
    allowed_ports = get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # For non-PDF files, route to single coolest worker
    if not is_pdf:
        # Use load balancer to pick best worker for this api_key
        coolest_port = load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        logger.info(f"[SYNC] Non-PDF file, routing to single worker: {coolest_port}")

        # Submit to worker
        task_id, error = await submit_to_worker(coolest_port, file_bytes, filename, to_formats)
        if error:
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        # Poll until complete
        poll_interval = 0.5
        while True:
            status, _ = await poll_worker_status(coolest_port, task_id)
            if status == "success":
                result, error = await get_worker_result(coolest_port, task_id)
                if error:
                    raise HTTPException(status_code=500, detail=f"Failed to get result: {error}")
                logger.info(f"[SYNC] Non-PDF complete")
                return result
            elif status == "failure":
                raise HTTPException(status_code=500, detail="Worker task failed")
            await asyncio.sleep(poll_interval)

    # PDF: Get page count
    try:
        total_pages = get_page_count(file_bytes)
        logger.info(f"[SYNC] PDF has {total_pages} pages")
    except Exception as e:
        logger.error(f"[SYNC] Invalid PDF: {e}")
        raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    # Parse user page range
    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    # Determine pages to process
    if user_range:
        start_page, end_page = user_range
        start_page = max(1, min(start_page, total_pages))
        end_page = max(start_page, min(end_page, total_pages))
        pages_to_process = end_page - start_page + 1
    else:
        start_page, end_page = 1, total_pages
        pages_to_process = total_pages

    # Split PDF
    try:
        chunks = split_pdf_for_workers(file_bytes, len(available_workers), user_range)
        logger.info(f"[SYNC] Split into {len(chunks)} chunks: {[c[1] for c in chunks]}")
    except Exception as e:
        logger.error(f"[SYNC] Error splitting PDF: {e}")
        raise HTTPException(status_code=500, detail=f"Error splitting PDF: {e}")

    # Submit chunks to workers and collect task IDs
    task_info = []  # List of (port, task_id, page_range)

    async def submit_chunk(chunk_data: Tuple[bytes, Tuple[int, int]], port: int):
        chunk_bytes, original_pages = chunk_data
        task_id, error = await submit_to_worker(port, chunk_bytes, filename, to_formats)
        if error:
            return None
        return (port, task_id, original_pages)

    # Submit all chunks in parallel
    tasks = []
    for chunk, port in zip(chunks, available_workers):
        tasks.append(submit_chunk(chunk, port))

    results = await asyncio.gather(*tasks)
    task_info = [r for r in results if r is not None]

    if not task_info:
        raise HTTPException(status_code=500, detail="Failed to submit to any worker")

    logger.info(f"[SYNC] Submitted to {len(task_info)} workers, polling...")

    # Update worker page counts (both current life and lifetime)
    for port in available_workers[:len(chunks)]:
        worker_id = get_worker_id(port)
        if worker_id in workers:
            pages_for_worker = pages_to_process // len(chunks)
            workers[worker_id]["current_life_pages"] = workers[worker_id].get("current_life_pages", 0) + pages_for_worker
            workers[worker_id]["lifetime_pages"] = workers[worker_id].get("lifetime_pages", 0) + pages_for_worker

    # Poll until all complete (with timeout)
    max_wait = 600  # 10 minutes
    poll_interval = 0.5
    start_time = time.time()
    completed_results = {}  # task_id -> result

    while len(completed_results) < len(task_info):
        if time.time() - start_time > max_wait:
            raise HTTPException(
                status_code=504,
                detail=f"Conversion timed out after {max_wait}s"
            )

        for port, task_id, page_range in task_info:
            if task_id in completed_results:
                continue

            status, _ = await poll_worker_status(port, task_id)

            if status == "success":
                result, error = await get_worker_result(port, task_id)
                if result:
                    completed_results[task_id] = {
                        "pages": page_range,
                        "result": result,
                    }
                    logger.info(f"[SYNC] Task {task_id[:8]} completed")
                else:
                    completed_results[task_id] = {
                        "pages": page_range,
                        "result": {"status": "failure", "errors": [{"message": error}]},
                    }
            elif status == "failure":
                completed_results[task_id] = {
                    "pages": page_range,
                    "result": {"status": "failure", "errors": [{"message": "Worker reported failure"}]},
                }

        if len(completed_results) < len(task_info):
            await asyncio.sleep(poll_interval)

    # Merge results
    sub_results = list(completed_results.values())
    merged = _merge_document_results(sub_results, filename)

    logger.info(f"[SYNC] Complete: {len(sub_results)} chunks merged, status={merged['status']}")

    return merged


def _aggregate_job_results(job) -> dict:
    """
    Aggregate results from all sub-jobs for internal stats tracking.

    Note: This is for INTERNAL use only. For client-facing results,
    use _build_merged_result() which returns standard docling-serve format.
    """
    aggregated = {
        "job_id": job.job_id,
        "filename": job.filename,
        "total_pages": job.total_pages,
        "processing_time": job.completed_at - job.started_at if job.completed_at and job.started_at else 0,
        "pages": {},
        "timings": {},
        "sub_results": [],
    }

    for sub_job in job.sub_jobs:
        if sub_job.result:
            aggregated["sub_results"].append({
                "pages": sub_job.original_pages,
                "result": sub_job.result,
            })

    return aggregated


def _build_merged_result(job) -> dict:
    """
    Build merged result in standard docling-serve format.

    Converts job sub_jobs into the same format a single worker would return.
    This is what clients see - they should not know about internal splitting.
    """
    # Build sub_results list from job's sub_jobs
    sub_results = []
    for sub_job in job.sub_jobs:
        if sub_job.result:
            sub_results.append({
                "pages": sub_job.original_pages,
                "result": sub_job.result,
            })

    # Use the same merge logic as sync endpoint
    return _merge_document_results(sub_results, job.filename)


@app.get("/v1/status/poll/{task_id}")
async def poll_status(task_id: str, _: str = Depends(verify_api_key)):
    """
    Poll job status.

    For split jobs, aggregates status from all sub-jobs.
    For chunk tasks, proxies to the worker.
    """
    # Check if this is a chunk task - proxy to worker
    if task_id in chunk_tasks:
        worker_port = chunk_tasks[task_id]
        status, _ = await poll_worker_status(worker_port, task_id)
        return {
            "task_id": task_id,
            "task_type": "chunk",
            "task_status": status,
            "task_position": None,
            "task_meta": None,
        }

    job = job_manager.get_job(task_id)

    if not job:
        # Check disk storage
        if result_storage.result_exists(task_id):
            return {"task_id": task_id, "task_status": "success"}
        raise HTTPException(status_code=404, detail="Job not found")

    # Check if job already failed (e.g., from timeout)
    if job.status == JobStatus.FAILED:
        duration = None
        if job.started_at:
            end_time = job.completed_at if job.completed_at else time.time()
            duration = round(end_time - job.started_at, 2)
        return {
            "task_id": task_id,
            "task_type": "convert",
            "task_status": "failure",
            "task_position": None,
            "task_meta": None,
            "error": job.error,
            "received_at": epoch_to_utc(job.received_at),
            "started_at": epoch_to_utc(job.started_at),
            "completed_at": epoch_to_utc(job.completed_at),
            "duration_seconds": duration,
        }

    # Check all sub-jobs
    all_complete = True
    any_failed = False
    sub_statuses = []

    for sub_job in job.sub_jobs:
        if sub_job.status == JobStatus.IN_PROGRESS and sub_job.worker_task_id:
            # Poll worker for actual status
            status, _ = await poll_worker_status(sub_job.worker_port, sub_job.worker_task_id)

            if status == "success":
                # Fetch result and mark complete
                result, error = await get_worker_result(sub_job.worker_port, sub_job.worker_task_id)
                if result:
                    job_manager.complete_sub_job(sub_job.sub_job_id, result=result)
                    logger.info(f"[JOB {task_id}] Worker {sub_job.worker_port} completed pages {sub_job.original_pages[0]}-{sub_job.original_pages[1]}")
                else:
                    job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
                    logger.warning(f"[JOB {task_id}] Worker {sub_job.worker_port} failed to return result: {error}")
                    any_failed = True
            elif status == "failure":
                job_manager.complete_sub_job(sub_job.sub_job_id, error="Worker task failed")
                logger.error(f"[JOB {task_id}] Worker {sub_job.worker_port} FAILED for pages {sub_job.original_pages[0]}-{sub_job.original_pages[1]}")
                any_failed = True
            else:
                all_complete = False

            sub_statuses.append({"pages": sub_job.original_pages, "status": status})

        elif sub_job.status == JobStatus.PENDING:
            all_complete = False
            sub_statuses.append({"pages": sub_job.original_pages, "status": "pending"})

        elif sub_job.status == JobStatus.COMPLETED:
            sub_statuses.append({"pages": sub_job.original_pages, "status": "success"})

        elif sub_job.status == JobStatus.FAILED:
            any_failed = True
            sub_statuses.append({"pages": sub_job.original_pages, "status": "failure"})

    # Determine overall status and save results when complete
    if all_complete and not any_failed:
        task_status = "success"
        # Save merged results to disk if not already saved
        if not result_storage.result_exists(task_id):
            merged = _build_merged_result(job)
            result_storage.save_result(task_id, merged)
            result_mb = len(str(merged)) / (1024 * 1024)
            duration_so_far = time.time() - job.started_at if job.started_at else 0
            logger.info(f"[JOB {task_id}] COMPLETE: {job.total_pages} pages, {len(job.sub_jobs)} workers, {duration_so_far:.1f}s, result={result_mb:.1f}MB")
    elif any_failed and all_complete:
        task_status = "failure"
        logger.error(f"[JOB {task_id}] FAILED: some workers failed")
    else:
        task_status = "started"

    # Calculate duration
    duration = None
    if job.started_at:
        end_time = job.completed_at if job.completed_at else time.time()
        duration = round(end_time - job.started_at, 2)

    # Return worker-compatible format with our timing additions
    return {
        "task_id": task_id,
        "task_type": "convert",
        "task_status": task_status,
        "task_position": None,
        "task_meta": None,
        # Our additions for timing visibility
        "received_at": epoch_to_utc(job.received_at),
        "started_at": epoch_to_utc(job.started_at),
        "completed_at": epoch_to_utc(job.completed_at),
        "duration_seconds": duration,
    }


@app.get("/v1/result/{task_id}")
async def get_result(task_id: str, _: str = Depends(verify_api_key)):
    """
    Get job result in standard docling-serve format.

    Returns the same format as a single worker would return.
    Falls back to disk storage if not in memory.
    For chunk tasks, proxies to the worker.
    """
    # Check if this is a chunk task - proxy to worker
    if task_id in chunk_tasks:
        worker_port = chunk_tasks[task_id]
        result, error = await get_worker_result(worker_port, task_id)
        if error:
            raise HTTPException(status_code=500, detail=f"Failed to get chunk result: {error}")
        # Clean up mapping after successful retrieval
        del chunk_tasks[task_id]
        return result

    # Check disk first (in case of restart)
    disk_result = result_storage.get_result(task_id)
    if disk_result:
        # Handle backward compat: old format has "sub_results", new format has "document"
        if "sub_results" in disk_result and "document" not in disk_result:
            # Old format - convert to merged format
            merged = _merge_document_results(disk_result["sub_results"], disk_result.get("filename", "document.pdf"))
            # Update disk with new format
            result_storage.save_result(task_id, merged)
            return merged
        return disk_result

    job = job_manager.get_job(task_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != JobStatus.COMPLETED:
        raise HTTPException(status_code=400, detail=f"Job not complete: {job.status.value}")

    # Build merged result in standard docling-serve format
    merged = _build_merged_result(job)

    # Save to disk for persistence
    result_storage.save_result(task_id, merged)

    return merged


# =============================================================================
# CHUNK ENDPOINTS (proxy to single worker)
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

    # Get available workers
    allowed_ports = get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # Use load balancer to pick best worker
    worker_port = load_balancer.select_single_worker(api_key)
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
            chunk_tasks[result["task_id"]] = worker_port
            logger.info(f"[CHUNK] Stored mapping: {result['task_id']} -> worker {worker_port}")

        return result


# Hierarchical chunking - sync
@app.post("/v1/chunk/hierarchical/file")
async def chunk_hierarchical_file(
    files: UploadFile = File(...),
    api_key: str = Depends(verify_api_key),
):
    """Hierarchical chunking of a document (sync)."""
    return await _proxy_chunk_request(files, "/v1/chunk/hierarchical/file", api_key)


# Hierarchical chunking - async
@app.post("/v1/chunk/hierarchical/file/async")
async def chunk_hierarchical_file_async(
    files: UploadFile = File(...),
    api_key: str = Depends(verify_api_key),
):
    """Hierarchical chunking of a document (async)."""
    return await _proxy_chunk_request(files, "/v1/chunk/hierarchical/file/async", api_key, is_async=True)


# Hybrid chunking - sync
@app.post("/v1/chunk/hybrid/file")
async def chunk_hybrid_file(
    files: UploadFile = File(...),
    api_key: str = Depends(verify_api_key),
):
    """Hybrid chunking of a document (sync)."""
    return await _proxy_chunk_request(files, "/v1/chunk/hybrid/file", api_key)


# Hybrid chunking - async
@app.post("/v1/chunk/hybrid/file/async")
async def chunk_hybrid_file_async(
    files: UploadFile = File(...),
    api_key: str = Depends(verify_api_key),
):
    """Hybrid chunking of a document (async)."""
    return await _proxy_chunk_request(files, "/v1/chunk/hybrid/file/async", api_key, is_async=True)


# -----------------------------------------------------------------------------
# Source (URL) chunk endpoints - proxy JSON body to worker
# -----------------------------------------------------------------------------

async def _proxy_chunk_source_request(
    request: Request,
    endpoint_path: str,
    api_key: str,
    is_async: bool = False,
) -> dict:
    """Proxy a chunk source (URL) request to a single worker."""
    # Get available workers
    allowed_ports = get_workers_for_api_key(api_key)
    available_workers = [
        p for p in allowed_ports
        if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
    ]

    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # Use load balancer to pick best worker
    worker_port = load_balancer.select_single_worker(api_key)
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
            chunk_tasks[result["task_id"]] = worker_port
            logger.info(f"[CHUNK] Stored mapping: {result['task_id']} -> worker {worker_port}")

        return result


# Hierarchical source - sync
@app.post("/v1/chunk/hierarchical/source")
async def chunk_hierarchical_source(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """Hierarchical chunking from URL source (sync)."""
    return await _proxy_chunk_source_request(request, "/v1/chunk/hierarchical/source", api_key)


# Hierarchical source - async
@app.post("/v1/chunk/hierarchical/source/async")
async def chunk_hierarchical_source_async(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """Hierarchical chunking from URL source (async)."""
    return await _proxy_chunk_source_request(request, "/v1/chunk/hierarchical/source/async", api_key, is_async=True)


# Hybrid source - sync
@app.post("/v1/chunk/hybrid/source")
async def chunk_hybrid_source(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """Hybrid chunking from URL source (sync)."""
    return await _proxy_chunk_source_request(request, "/v1/chunk/hybrid/source", api_key)


# Hybrid source - async
@app.post("/v1/chunk/hybrid/source/async")
async def chunk_hybrid_source_async(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """Hybrid chunking from URL source (async)."""
    return await _proxy_chunk_source_request(request, "/v1/chunk/hybrid/source/async", api_key, is_async=True)


# =============================================================================
# STATS ENDPOINT
# =============================================================================

@app.get("/stats")
async def get_stats(_: str = Depends(verify_api_key)):
    """Get processing statistics."""
    job_stats = job_manager.get_stats()
    storage_stats = result_storage.get_storage_stats()
    pool_status = load_balancer.get_pool_status()

    return {
        "jobs": job_stats,
        "storage": storage_stats,
        "workers": pool_status,
        "uptime": time.time() - app.state.start_time if hasattr(app.state, "start_time") else 0,
    }


# =============================================================================
# WORKER MANAGEMENT ENDPOINTS
# =============================================================================

@app.get("/status")
async def get_status(_: str = Depends(verify_api_key)):
    """Get status of all workers."""
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


@app.get("/health")
async def get_health_simple():
    """Simple health check (no auth required, for load balancers)."""
    return {"status": "ok"}


@app.get("/version")
async def get_version():
    """Version info (no auth required)."""
    import platform
    from importlib.metadata import version
    return {
        "controller": "2.0.0",
        "docling-serve": version("docling-serve"),
        "docling": version("docling"),
        "python": f"cpython-{platform.python_version_tuple()[0]}{platform.python_version_tuple()[1]} ({platform.python_version()})",
        "platform": platform.platform(),
    }


@app.post("/worker/add")
async def add_worker(port: Optional[int] = None, _: str = Depends(verify_api_key)):
    """Add a new worker."""
    if port is None:
        used_ports = {w["port"] for w in workers.values()}
        for p in range(MIN_PORT, MAX_PORT + 1):
            if p not in used_ports:
                port = p
                break
        if port is None:
            raise HTTPException(status_code=503, detail="No available ports")

    worker_id = get_worker_id(port)
    if worker_id in workers:
        raise HTTPException(status_code=409, detail=f"Worker {worker_id} already exists")

    await init_worker(port)
    return {"worker_id": worker_id, "port": port, "added": True}


@app.post("/worker/{worker_id}/restart")
async def restart_worker_endpoint(worker_id: str, _: str = Depends(verify_api_key)):
    """Restart a specific worker."""
    if worker_id not in workers:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    success = await restart_worker(worker_id, reason="manual")
    return {"worker_id": worker_id, "restarted": success}


@app.post("/worker/{worker_id}/remove")
async def remove_worker_endpoint(worker_id: str, _: str = Depends(verify_api_key)):
    """Remove a worker."""
    if worker_id not in workers:
        raise HTTPException(status_code=404, detail=f"Worker {worker_id} not found")

    success = await remove_worker(worker_id)
    return {"worker_id": worker_id, "removed": success}


# =============================================================================
# DOCS PASSTHROUGH
# =============================================================================

@app.get("/docs", include_in_schema=False)
async def docs_redirect():
    """Redirect /docs to worker's docs."""
    return RedirectResponse(f"/{MIN_PORT}/docs")


@app.get("/redoc", include_in_schema=False)
async def redoc_redirect():
    """Redirect /redoc to worker's redoc."""
    return RedirectResponse(f"/{MIN_PORT}/redoc")


@app.get("/openapi.json", include_in_schema=False)
async def openapi_redirect():
    """Redirect /openapi.json to worker's OpenAPI spec."""
    return RedirectResponse(f"/{MIN_PORT}/openapi.json")


# =============================================================================
# ROUND-ROBIN PROXY (for remaining /v1/* endpoints)
# =============================================================================

@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def v1_proxy(path: str, request: Request, api_key: str = Depends(verify_api_key)):
    """
    Round-robin proxy for /v1/* endpoints not explicitly handled.
    Routes to a worker based on API key tier.
    """
    global _rr_index

    # Get allowed workers for this API key
    allowed_ports = get_workers_for_api_key(api_key)
    ready_workers = [
        p for p in allowed_ports
        if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
    ]

    if not ready_workers:
        raise HTTPException(status_code=503, detail="No workers available")

    # Round-robin selection
    port = ready_workers[_rr_index % len(ready_workers)]
    _rr_index += 1

    target_url = f"http://{WORKER_HOST}:{port}/v1/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()

    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                content=body,
                headers={k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers={k: v for k, v in resp.headers.items() if k.lower() not in ("content-encoding", "transfer-encoding", "content-length")}
            )
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Worker on port {port} not responding")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Worker on port {port} timed out")


# =============================================================================
# LEGACY PROXY (for direct worker access)
# =============================================================================

@app.api_route("/{port:int}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"])
async def proxy(port: int, path: str, request: Request, api_key: str = Depends(verify_api_key)):
    """Direct proxy to worker (bypasses load balancing)."""
    # Validate port is allowed for this API key's tier
    allowed_ports = get_workers_for_api_key(api_key)
    if port not in allowed_ports:
        raise HTTPException(status_code=403, detail=f"Port {port} not allowed for your API key tier")

    worker_id = get_worker_id(port)
    if worker_id not in workers:
        raise HTTPException(status_code=404, detail=f"No worker on port {port}")

    target_url = f"http://{WORKER_HOST}:{port}/{path}"
    if request.url.query:
        target_url += f"?{request.url.query}"

    body = await request.body()

    try:
        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                content=body,
                headers={k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            )
            return Response(
                content=resp.content,
                status_code=resp.status_code,
                headers={k: v for k, v in resp.headers.items() if k.lower() not in ("content-encoding", "transfer-encoding", "content-length")}
            )
    except httpx.ConnectError:
        raise HTTPException(status_code=502, detail=f"Worker on port {port} not responding")
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail=f"Worker on port {port} timed out")


# =============================================================================
# BACKGROUND TASKS
# =============================================================================

async def watchdog():
    """Monitor workers and enforce restart policies."""
    while True:
        try:
            logger.info(f"[WATCHDOG] === Starting watchdog iteration, {len(workers)} workers ===")
            for worker_id, worker in list(workers.items()):
                pid = worker["pid"]
                port = worker["port"]
                logger.info(f"[WATCHDOG] Processing {worker_id}: port={port}, state={worker['state']}, life_pages={worker.get('current_life_pages', 0)}, lifetime={worker.get('lifetime_pages', 0)}, restarts={worker.get('restart_count', 0)}")

                # Check if process is alive
                process_exists = pid and psutil.pid_exists(pid)
                logger.info(f"[WATCHDOG] {worker_id}: pid={pid}, exists={process_exists}")
                if not process_exists:
                    logger.info(f"[WATCHDOG] {worker_id}: PROCESS DEAD! Triggering restart...")
                    print(f"Watchdog: {worker_id} process dead, restarting...")
                    await restart_worker(worker_id, reason="process_died")
                    continue

                # Check memory limit
                try:
                    proc = psutil.Process(pid)
                    memory_mb = proc.memory_info().rss / (1024 * 1024)
                    logger.info(f"[WATCHDOG] {worker_id}: memory={memory_mb:.0f}MB, max={MAX_MEMORY_MB}MB")

                    # Update worker memory in load balancer
                    workers[worker_id]["memory_mb"] = memory_mb
                    load_balancer.update_worker(port, workers[worker_id])

                    if memory_mb > MAX_MEMORY_MB:
                        # Check for pending jobs before restart
                        pending = job_manager.get_pending_jobs_for_worker(port)
                        if not pending:
                            logger.info(f"[WATCHDOG] {worker_id}: MEMORY LIMIT! Triggering restart...")
                            print(f"Watchdog: {worker_id} memory {memory_mb:.0f}MB > {MAX_MEMORY_MB}MB, restarting...")
                            await restart_worker(worker_id, reason="memory_limit")
                            continue
                        else:
                            print(f"Watchdog: {worker_id} over memory limit but has pending jobs, waiting...")

                except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                    logger.info(f"[WATCHDOG] {worker_id}: psutil exception: {e}")

                # Check page count limit (based on current life pages, not lifetime)
                current_life_pages = worker.get("current_life_pages", 0)
                if current_life_pages >= MAX_PAGES_BEFORE_RESTART:
                    logger.info(f"[WATCHDOG] {worker_id}: current_life_pages={current_life_pages} >= {MAX_PAGES_BEFORE_RESTART}, triggering restart...")
                    if worker["state"] != "processing":
                        logger.info(f"[WATCHDOG] {worker_id}: state={worker['state']} != processing, polling pending sub-jobs...")
                        # Poll pending jobs to detect completion and save results
                        pending_sub_jobs = job_manager.get_pending_sub_jobs_for_worker(port)
                        logger.info(f"[WATCHDOG] {worker_id}: got {len(pending_sub_jobs)} pending sub-jobs for port {port}")
                        for job_id, sub_job in pending_sub_jobs:
                            if not sub_job.worker_task_id:
                                logger.info(f"[WATCHDOG] {worker_id}: sub_job {sub_job.sub_job_id[:8]} has no worker_task_id, skipping")
                                continue
                            try:
                                logger.info(f"[WATCHDOG] {worker_id}: polling task {sub_job.worker_task_id[:8]} on port {port}...")
                                status, _ = await poll_worker_status(port, sub_job.worker_task_id)
                                logger.info(f"[WATCHDOG] {worker_id}: task {sub_job.worker_task_id[:8]} status={status}")
                                if status == "success":
                                    result, error = await get_worker_result(port, sub_job.worker_task_id)
                                    if result:
                                        job_manager.complete_sub_job(sub_job.sub_job_id, result=result)
                                        logger.info(f"[WATCHDOG] {worker_id}: completed sub_job {sub_job.sub_job_id[:8]}")
                                    else:
                                        job_manager.complete_sub_job(sub_job.sub_job_id, error=error or "Failed to fetch")
                                        logger.info(f"[WATCHDOG] {worker_id}: failed sub_job {sub_job.sub_job_id[:8]}: {error}")
                                elif status == "failure":
                                    job_manager.complete_sub_job(sub_job.sub_job_id, error="Worker task failed")
                                    logger.info(f"[WATCHDOG] {worker_id}: task failed for sub_job {sub_job.sub_job_id[:8]}")
                            except Exception as e:
                                logger.warning(f"Watchdog poll error for {job_id}: {e}")

                        # Save completed parent jobs to disk
                        logger.info(f"[WATCHDOG] {worker_id}: checking {len(pending_sub_jobs)} parent jobs for disk save...")
                        for job_id, _ in pending_sub_jobs:
                            job = job_manager.get_job(job_id)
                            if job:
                                logger.info(f"[WATCHDOG] {worker_id}: job {job_id[:8]} status={job.status.value}, exists_on_disk={result_storage.result_exists(job_id)}")
                            if job and job.status == JobStatus.COMPLETED and not result_storage.result_exists(job_id):
                                merged = _build_merged_result(job)
                                result_storage.save_result(job_id, merged)
                                logger.info(f"Watchdog saved job {job_id} to disk")

                        # Re-check pending after polling
                        pending = job_manager.get_pending_jobs_for_worker(port)
                        logger.info(f"[WATCHDOG] {worker_id}: re-check pending for port {port} = {len(pending)} jobs")
                        if not pending:
                            logger.info(f"[WATCHDOG] {worker_id}: current_life_pages={current_life_pages} >= {MAX_PAGES_BEFORE_RESTART}, restarting...")
                            await restart_worker(worker_id, reason=f"page_limit ({current_life_pages} pages)")
                            continue
                    else:
                        logger.info(f"[WATCHDOG] {worker_id}: state={worker['state']} == processing, skipping restart check")

                # Check idle timeout
                idle_time = time.time() - worker["last_activity"]
                if idle_time > IDLE_RESTART_SEC and worker["state"] == "ready":
                    logger.info(f"[WATCHDOG] {worker_id}: idle for {idle_time:.0f}s > {IDLE_RESTART_SEC}s, restarting...")
                    await restart_worker(worker_id, reason="idle_timeout")
                    continue

            await asyncio.sleep(5)

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Watchdog error: {e}")
            await asyncio.sleep(5)


async def cleanup_task():
    """Periodic cleanup of old jobs and files."""
    while True:
        try:
            # Timeout stuck jobs (mark as failed if IN_PROGRESS too long)
            timed_out = job_manager.timeout_stuck_jobs(timeout_seconds=JOB_TIMEOUT_SEC)
            if timed_out > 0:
                logger.warning(f"Timed out {timed_out} stuck job(s)")

            # Cleanup old jobs from memory
            removed_jobs = job_manager.cleanup_old_jobs(max_age_seconds=3600)

            # Cleanup old files from disk
            deleted_files = result_storage.cleanup_old_files(max_age_seconds=3600)

            # Cleanup old uploads
            deleted_uploads = cleanup_old_uploads(max_age_seconds=3600)

            if removed_jobs > 0 or sum(deleted_files.values()) > 0 or deleted_uploads > 0:
                print(f"Cleanup: removed {removed_jobs} jobs, {deleted_files} files, {deleted_uploads} uploads")

            # Save stats periodically
            stats = job_manager.get_stats()
            result_storage.save_stats(stats)

            # Save worker stats periodically (survives controller restart)
            worker_stats = {}
            for worker_id, worker in workers.items():
                worker_stats[str(worker["port"])] = {
                    "current_life_pages": worker.get("current_life_pages", 0),
                    "lifetime_pages": worker.get("lifetime_pages", 0),
                    "restart_count": worker.get("restart_count", 0),
                    "last_restart_at": worker.get("last_restart_at"),
                    "last_restart_reason": worker.get("last_restart_reason"),
                    "started_at": worker.get("started_at"),
                    "last_activity": worker.get("last_activity"),
                }
            result_storage.save_worker_stats(worker_stats)

            await asyncio.sleep(300)  # Every 5 minutes

        except asyncio.CancelledError:
            break
        except Exception as e:
            print(f"Cleanup error: {e}")
            await asyncio.sleep(300)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
