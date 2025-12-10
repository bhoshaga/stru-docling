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
from fastapi.middleware.cors import CORSMiddleware
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
    extract_pages,
    job_manager,
    JobStatus,
    load_balancer,
    result_storage,
    upload_router,
    init_upload_router,
    cleanup_old_uploads,
    uploaded_files,
    stats_router,
    init_stats_router,
    merge_document_results,
    chunk_router,
    init_chunk_router,
    # Job queue
    JobRequest,
    init_job_queue,
    enqueue_job,
    job_queue_consumer,
    worker_router,
    init_worker_router,
    submit_chunk_with_retry,
    validate_file_type,
    validate_conversion_params,
)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Platform detection
import platform
IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"



# Watchdog settings
MAX_PAGES_BEFORE_RESTART = int(os.environ.get("MAX_PAGES_BEFORE_RESTART", "10"))
MAX_MEMORY_MB = int(os.environ.get("MAX_MEMORY_MB", "12000"))
# MAX_UNAVAILABLE is set dynamically after NUM_WORKERS (see below)
REQUEST_TIMEOUT_SEC = int(os.environ.get("REQUEST_TIMEOUT_SEC", "600"))
IDLE_RESTART_SEC = int(os.environ.get("IDLE_RESTART_SEC", "3600"))
JOB_TIMEOUT_SEC = int(os.environ.get("JOB_TIMEOUT_SEC", "600"))  # 10 minutes

# Worker settings
WORKER_HOST = "127.0.0.1"
NUM_THREADS_PER_WORKER = int(os.environ.get("DOCLING_SERVE_ENG_LOC_NUM_WORKERS", "2"))
NUM_WORKERS = int(os.environ.get("NUM_WORKERS", "10"))
MIN_PORT = 5001
MAX_PORT = MIN_PORT + NUM_WORKERS - 1

# Dynamic MAX_UNAVAILABLE: 30% of workers, minimum 1, can be overridden by env
_default_max_unavailable = max(1, int(NUM_WORKERS * 0.3))
MAX_UNAVAILABLE = int(os.environ.get("MAX_UNAVAILABLE", str(_default_max_unavailable)))

# API authentication - Two tiers with limits (dynamic allocation)
# Default: 80% dedicated, 20% shared (minimum 1 shared)
# Override with SHARED_WORKERS env var (set to 0 for all dedicated)
_default_shared = max(1, int(NUM_WORKERS * 0.2))
_num_shared = int(os.environ.get("SHARED_WORKERS", str(_default_shared)))
_num_dedicated = NUM_WORKERS - _num_shared
_dedicated_ports = list(range(MIN_PORT, MIN_PORT + _num_dedicated))
_shared_ports = list(range(MIN_PORT + _num_dedicated, MAX_PORT + 1))

API_KEYS = {
    "windowseat": {
        "tier": "dedicated",
        "workers": _dedicated_ports,  # 80% of workers
        "max_file_mb": 200,  # Max file size in MB
        "max_pages": 400,    # Max pages per PDF
    },
    "middleseat": {
        "tier": "shared",
        "workers": _shared_ports,  # 20% of workers
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
                                    # Current life stats - always start at 0 (we don't know worker's actual state)
                                    "current_life_pages": 0,
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
                                    # Restart coordination
                                    "restart_pending": False,
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

    # Initialize upload router with dependencies (only needs auth and limits)
    init_upload_router(
        verify_api_key_func=verify_api_key,
        check_file_limits_func=check_file_limits,
    )

    # Initialize stats router with dependencies
    init_stats_router(
        verify_api_key_func=verify_api_key,
        get_workers_func=lambda: workers,
        get_start_time_func=lambda: getattr(app.state, "start_time", None),
    )

    # Initialize chunk router with dependencies
    init_chunk_router(
        verify_api_key_func=verify_api_key,
        check_file_limits_func=check_file_limits,
        get_available_workers_func=get_available_workers,
        load_balancer=load_balancer,
        chunk_tasks_dict=chunk_tasks,
    )

    # Initialize worker router with dependencies
    init_worker_router(
        verify_api_key_func=verify_api_key,
        workers_dict=workers,
        min_port=MIN_PORT,
        max_port=MAX_PORT,
        get_worker_id_func=get_worker_id,
        init_worker_func=init_worker,
        restart_worker_func=restart_worker,
        remove_worker_func=remove_worker,
    )

    # Initialize job queue with dependencies
    init_job_queue(
        get_available_workers_func=get_available_workers,
        split_pdf_for_workers_func=split_pdf_for_workers,
        submit_chunk_with_retry_func=submit_chunk_with_retry,
        submit_chunk_with_page_split_func=submit_chunk_with_page_split,
        submit_to_worker_func=submit_to_worker,
        poll_worker_status_func=poll_worker_status,
        get_worker_result_func=get_worker_result,
        get_workers_for_api_key_func=get_workers_for_api_key,
        get_worker_id_func=get_worker_id,
        workers_dict=workers,
        job_manager_instance=job_manager,
        load_balancer_instance=load_balancer,
        max_pages_before_restart=MAX_PAGES_BEFORE_RESTART,
    )

    # Start background tasks
    background_tasks.append(asyncio.create_task(job_queue_consumer()))  # Job queue consumer
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

app = FastAPI(title="Docling Controller", version="2.0.0", lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None)

# CORS middleware - allow cross-origin requests from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, OPTIONS, etc.)
    allow_headers=["*"],  # Allow all headers (including Authorization)
)

# Include routers
app.include_router(upload_router)
app.include_router(stats_router)
app.include_router(chunk_router)
app.include_router(worker_router)

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


def check_file_limits(file_bytes: bytes, api_key: str, filename: str = "", is_pdf: bool = True) -> Optional[str]:
    """
    Check if file meets tier limits and is a supported type.
    Returns error message if invalid, None if OK.
    """
    # Check file type is supported
    if filename:
        type_error = validate_file_type(filename)
        if type_error:
            return type_error

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


async def get_available_workers(api_key: str, max_wait: int = 60) -> List[int]:
    """
    Get available workers for an API key with detailed logging.

    Workers are available if:
    - They exist in the workers dict
    - Their state is "ready" (not "processing" or "restarting")
    - They are NOT tagged for restart (restart_pending=False)

    Before selecting, tags workers at page limit for restart (respecting MAX_UNAVAILABLE).

    Returns list of available worker ports.
    """
    allowed_ports = get_workers_for_api_key(api_key)
    wait_start = time.time()

    while time.time() - wait_start < max_wait:
        # Tag workers at page limit for restart (respects MAX_UNAVAILABLE)
        tag_workers_for_restart(allowed_ports)

        available = []
        unavailable_reasons = []

        for port in allowed_ports:
            worker_id = get_worker_id(port)
            if worker_id not in workers:
                unavailable_reasons.append(f"{worker_id}: not registered")
                continue

            worker = workers[worker_id]
            state = worker.get("state", "unknown")
            pages = worker.get("current_life_pages", 0)
            restart_pending = worker.get("restart_pending", False)

            # Workers with restart_pending are unavailable (tagged by tag_workers_for_restart)
            if restart_pending:
                unavailable_reasons.append(f"{worker_id}: restart_pending (pages={pages})")
            elif state == "ready":
                available.append(port)
            else:
                unavailable_reasons.append(f"{worker_id}: state={state}, pages={pages}")

        if available:
            # Log summary of available workers
            available_info = []
            for port in available:
                w = workers[get_worker_id(port)]
                available_info.append(f"{get_worker_id(port)}(pages={w.get('current_life_pages', 0)})")
            logger.info(f"[WORKERS] Available: {', '.join(available_info)}")
            if unavailable_reasons:
                logger.info(f"[WORKERS] Unavailable: {', '.join(unavailable_reasons)}")
            return available

        # No workers available - log why and wait
        logger.info(f"[WORKERS] None available, waiting... ({time.time() - wait_start:.0f}s)")
        for reason in unavailable_reasons:
            logger.info(f"[WORKERS]   {reason}")
        await asyncio.sleep(2)

    # Timeout - log final state
    logger.warning(f"[WORKERS] Timeout after {max_wait}s, no workers available")
    return []


def tag_workers_for_restart(allowed_ports: List[int]) -> None:
    """
    Tag workers that have reached page limit for restart.

    Called during job assignment to coordinate restarts with MAX_UNAVAILABLE.
    Tags highest-memory worker at limit, respecting MAX_UNAVAILABLE cap.

    Tagged workers are excluded from job assignment and will be restarted
    by the watchdog when they become idle (state="ready").
    """
    # Count currently tagged/restarting workers (unavailable)
    unavailable_count = sum(
        1 for w in workers.values()
        if w.get("restart_pending") or w.get("state") == "restarting"
    )

    logger.info(f"[RESTART_TAG] Check: unavailable={unavailable_count}, MAX_UNAVAILABLE={MAX_UNAVAILABLE}, allowed_ports={allowed_ports}")

    if unavailable_count >= MAX_UNAVAILABLE:
        logger.info(f"[RESTART_TAG] Skipping - already at max unavailable")
        return  # Already at max unavailable, don't tag more

    # Find workers at page limit that aren't already tagged
    candidates = []
    for port in allowed_ports:
        worker_id = get_worker_id(port)
        if worker_id not in workers:
            logger.info(f"[RESTART_TAG] {worker_id}: not in workers dict")
            continue
        worker = workers[worker_id]
        pages = worker.get("current_life_pages", 0)
        logger.info(f"[RESTART_TAG] {worker_id}: pages={pages}, restart_pending={worker.get('restart_pending')}, state={worker.get('state')}, limit={MAX_PAGES_BEFORE_RESTART}")
        if worker.get("restart_pending"):
            continue  # Already tagged
        if worker.get("state") == "restarting":
            continue  # Already restarting
        if pages >= MAX_PAGES_BEFORE_RESTART:
            candidates.append((worker_id, worker))

    logger.info(f"[RESTART_TAG] Candidates: {[c[0] for c in candidates]}")
    if not candidates:
        return  # No workers at limit

    # Sort by memory (highest first) - memory is the critical resource
    candidates.sort(key=lambda x: x[1].get("memory_mb", 0), reverse=True)

    # Tag workers up to MAX_UNAVAILABLE
    for worker_id, worker in candidates:
        if unavailable_count >= MAX_UNAVAILABLE:
            break
        worker["restart_pending"] = True
        unavailable_count += 1
        logger.info(f"[RESTART_TAG] {worker_id}: tagged for restart (pages={worker.get('current_life_pages', 0)}, memory={worker.get('memory_mb', 0):.0f}MB, unavailable={unavailable_count}/{MAX_UNAVAILABLE})")


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


def get_real_pid_on_port(port: int) -> int | None:
    """Get the actual PID of the process listening on a port using lsof.

    This is the source of truth - more reliable than stored PIDs which can
    become stale (zombie processes, restarts, etc).
    """
    try:
        result = subprocess.run(
            ["lsof", "-ti", f":{port}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        logger.debug(f"[PID_CHECK] lsof :{port} -> returncode={result.returncode}, stdout='{result.stdout.strip()}', stderr='{result.stderr.strip()}'")
        if result.returncode == 0 and result.stdout.strip():
            # lsof may return multiple PIDs, take the first one
            pid_str = result.stdout.strip().split('\n')[0]
            logger.debug(f"[PID_CHECK] port {port} -> PID {pid_str}")
            return int(pid_str)
        # Log why we're returning None
        if result.returncode != 0:
            logger.info(f"[PID_CHECK] port {port}: lsof returned code {result.returncode} (no process listening)")
        elif not result.stdout.strip():
            logger.info(f"[PID_CHECK] port {port}: lsof returned empty output (no process listening)")
        return None
    except subprocess.TimeoutExpired:
        logger.warning(f"[PID_CHECK] lsof timed out for port {port}")
        return None
    except Exception as e:
        logger.warning(f"[PID_CHECK] lsof failed for port {port}: {type(e).__name__}: {e}")
        return None


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
    current_life_pages = worker.get("current_life_pages", 0)
    lifetime_pages = worker.get("lifetime_pages", 0)

    logger.info("=" * 60)
    logger.info(f"[WORKER RESTART] {worker_id} (port {port}) - STARTING")
    logger.info(f"[WORKER RESTART] Reason: {reason}")
    logger.info(f"[WORKER RESTART] Old PID: {old_pid}, current_life_pages={current_life_pages}, lifetime_pages={lifetime_pages}")
    logger.info("=" * 60)

    # Mark as restarting to block new task assignments
    workers[worker_id]["state"] = "restarting"
    load_balancer.update_worker(port, workers[worker_id])
    logger.info(f"[WORKER RESTART] {worker_id}: Saving pending results before restart...")

    # Poll and save all pending sub-jobs before restart
    pending_sub_jobs = job_manager.get_pending_sub_jobs_for_worker(port)
    logger.info(f"[RESTART] {worker_id}: got {len(pending_sub_jobs)} pending sub-jobs to poll before restart")
    for job_id, sub_job in pending_sub_jobs:
        logger.info(f"[RESTART] {worker_id}: checking sub_job {sub_job.sub_job_id}, task_id={sub_job.worker_task_id}")
        if not sub_job.worker_task_id:
            logger.info(f"[RESTART] {worker_id}: sub_job has no task_id, skipping")
            continue
        try:
            # Poll worker for status
            logger.info(f"[RESTART] {worker_id}: polling task {sub_job.worker_task_id} on port {port}...")
            status, _ = await poll_worker_status(port, sub_job.worker_task_id)
            logger.info(f"[RESTART] {worker_id}: task {sub_job.worker_task_id} status={status}")
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
                logger.info(f"[RESTART] {worker_id}: task failed for sub_job {sub_job.sub_job_id}")
            # If still processing, we'll lose this result - log warning
            elif status == "started":
                logger.warning(f"Sub-job {sub_job.sub_job_id} still processing, may lose result on restart")
            else:
                logger.warning(f"[RESTART] {worker_id}: unexpected status '{status}' for task {sub_job.worker_task_id}")
        except Exception as e:
            logger.warning(f"Could not save pending result for {job_id}: {e}")

    # Now check if any parent jobs completed and save to disk
    for job_id, _ in pending_sub_jobs:
        job = job_manager.get_job(job_id)
        if job and job.status == JobStatus.SUCCESS and not result_storage.result_exists(job_id):
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
        # Restart coordination - clear the tag
        "restart_pending": False,
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
    image_export_mode: str = "embedded",
    include_images: bool = True,
    do_ocr: bool = True,
    force_ocr: bool = False,
    ocr_engine: str = "easyocr",
    ocr_lang: Optional[str] = None,
    do_table_structure: bool = True,
    table_mode: str = "fast",
    pipeline: str = "standard",
    vlm_pipeline_model: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Submit a PDF chunk to a worker.

    Returns:
        (task_id, error) - task_id if successful, error message if failed
    """
    url = f"http://{WORKER_HOST}:{port}/v1/convert/file/async"
    logger.info(f"Submitting to worker {port}: {len(pdf_bytes)} bytes, format={to_formats}, "
                f"do_ocr={do_ocr}, force_ocr={force_ocr}, ocr_engine={ocr_engine}, ocr_lang={ocr_lang}, "
                f"do_table_structure={do_table_structure}, table_mode={table_mode}, pipeline={pipeline}, "
                f"vlm_pipeline_model={vlm_pipeline_model}, "
                f"image_export_mode={image_export_mode}, include_images={include_images}")

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
            data = {
                "to_formats": to_formats,
                "image_export_mode": image_export_mode,
                "include_images": str(include_images).lower(),
                "do_ocr": str(do_ocr).lower(),
                "force_ocr": str(force_ocr).lower(),
                "ocr_engine": ocr_engine,
                "do_table_structure": str(do_table_structure).lower(),
                "table_mode": table_mode,
                "pipeline": pipeline,
            }
            if ocr_lang:
                data["ocr_lang"] = ocr_lang
            if vlm_pipeline_model:
                data["vlm_pipeline_model"] = vlm_pipeline_model

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
                error = f"Worker returned {response.status_code}: {response.text}"
                logger.error(f"Worker {port} error: {error}")
                # Set state to failed so watchdog can restart
                if worker_id in workers:
                    workers[worker_id]["state"] = "failed"
                    load_balancer.update_worker(port, workers[worker_id])
                return None, error

    except httpx.TimeoutException:
        error = f"Timeout submitting to worker on port {port}"
        logger.error(error)
        # Set state to failed so watchdog can restart
        if worker_id in workers:
            workers[worker_id]["state"] = "failed"
            load_balancer.update_worker(port, workers[worker_id])
        return None, error
    except httpx.ConnectError:
        error = f"Cannot connect to worker on port {port}"
        logger.error(error)
        # Set state to failed so watchdog can restart
        if worker_id in workers:
            workers[worker_id]["state"] = "failed"
            load_balancer.update_worker(port, workers[worker_id])
        return None, error
    except Exception as e:
        logger.error(f"Worker {port} exception: {e}")
        # Set state to failed so watchdog can restart
        if worker_id in workers:
            workers[worker_id]["state"] = "failed"
            load_balancer.update_worker(port, workers[worker_id])
        return None, str(e)


async def poll_worker_status(port: int, task_id: str) -> Tuple[str, Optional[dict]]:
    """
    Poll a worker for task status.

    Returns:
        (status, result_if_complete)

    Raises:
        Exception if worker is unreachable (connection error, timeout, etc.)
    """
    url = f"http://{WORKER_HOST}:{port}/v1/status/poll/{task_id}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(url)
        if response.status_code == 200:
            data = response.json()
            return data.get("task_status", "unknown"), data
        else:
            raise Exception(f"Worker {port} returned status {response.status_code}")


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


# Thread pool for CPU-bound PDF operations
_pdf_executor = ThreadPoolExecutor(max_workers=4)


async def submit_chunk_with_page_split(
    port: int,
    chunk_bytes: bytes,
    original_pages: Tuple[int, int],
    filename: str,
    to_formats: str,
    image_export_mode: str = "embedded",
    include_images: bool = True,
    do_ocr: bool = True,
    force_ocr: bool = False,
    ocr_engine: str = "easyocr",
    ocr_lang: Optional[str] = None,
    do_table_structure: bool = True,
    table_mode: str = "fast",
    pipeline: str = "standard",
    vlm_pipeline_model: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Submit a chunk by splitting into individual pages, running in parallel on the same worker,
    then merging results. Transparent to caller - looks like single chunk submission.

    Args:
        port: Worker port
        chunk_bytes: PDF bytes for this chunk
        original_pages: (start_page, end_page) 1-indexed
        filename: Original filename
        to_formats: Output format

    Returns:
        (merged_result, error) - merged result if successful, error message if failed
    """
    start_page, end_page = original_pages
    num_pages = end_page - start_page + 1

    # If only 1 page, no split needed - direct submission + poll + get result
    if num_pages == 1:
        task_id, error = await submit_to_worker(port, chunk_bytes, filename, to_formats, image_export_mode, include_images, do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline, vlm_pipeline_model)
        if error:
            return None, error

        # Poll until complete
        try:
            while True:
                status, _ = await poll_worker_status(port, task_id)
                if status == "success":
                    result, err = await get_worker_result(port, task_id)
                    # Set worker state back to ready
                    worker_id = get_worker_id(port)
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        workers[worker_id]["last_activity"] = time.time()
                        load_balancer.update_worker(port, workers[worker_id])
                    return result, err
                elif status == "failure":
                    # Set worker state back to ready even on failure
                    worker_id = get_worker_id(port)
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        load_balancer.update_worker(port, workers[worker_id])
                    return None, f"Worker task {task_id} failed"
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.warning(f"[PAGE_SPLIT] Worker {port} unreachable: {e}")
            return None, f"Worker {port} unreachable: {e}"

    # Multiple pages: split, submit all, poll all, merge
    chunk_start_time = time.time()
    logger.info(f"[PAGE_SPLIT] Splitting {num_pages} pages for worker {port} (pages {start_page}-{end_page})")

    # Extract each page in parallel using ThreadPoolExecutor (CPU-bound)
    extract_start = time.time()
    loop = asyncio.get_event_loop()
    extract_tasks = [
        loop.run_in_executor(_pdf_executor, extract_pages, chunk_bytes, i + 1, i + 1)
        for i in range(num_pages)
    ]
    try:
        page_bytes_list = await asyncio.gather(*extract_tasks)
    except Exception as e:
        logger.error(f"[PAGE_SPLIT] Failed to extract pages: {e}")
        return None, f"Page extraction failed: {e}"
    extract_elapsed = time.time() - extract_start

    logger.info(f"[TIMING] Worker {port}: page_extract={extract_elapsed:.3f}s for {len(page_bytes_list)} pages")

    # Submit all pages in parallel (I/O-bound)
    submit_start = time.time()
    async def submit_single_page(page_bytes: bytes, page_num: int):
        task_id, error = await submit_to_worker(port, page_bytes, filename, to_formats, image_export_mode, include_images, do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline, vlm_pipeline_model)
        return (task_id, page_num, error)

    submit_tasks = [
        submit_single_page(pb, start_page + i)
        for i, pb in enumerate(page_bytes_list)
    ]
    submissions = await asyncio.gather(*submit_tasks)
    submit_elapsed = time.time() - submit_start

    # Check for submission failures - track failed pages
    successful_submissions = [(tid, pn) for tid, pn, err in submissions if tid and not err]
    failed_submissions = [(pn, err) for tid, pn, err in submissions if err]
    failed_pages = []  # Track all failed pages with reasons

    if failed_submissions:
        logger.warning(f"[PAGE_SPLIT] {len(failed_submissions)} pages failed to submit: {failed_submissions}")
        for pn, err in failed_submissions:
            failed_pages.append({"page": pn, "reason": f"submission_failed: {err}"})

    if not successful_submissions:
        return None, "All page submissions failed"

    logger.info(f"[TIMING] Worker {port}: http_submit={submit_elapsed:.3f}s for {len(successful_submissions)} pages")

    # Poll all tasks until complete (this is where library processing happens)
    poll_start = time.time()
    pending = {tid: pn for tid, pn in successful_submissions}
    completed = {}  # task_id -> (page_num, result)
    max_wait = 300  # 5 minutes
    start_time = time.time()

    try:
        while pending and (time.time() - start_time) < max_wait:
            for task_id in list(pending.keys()):
                status, _ = await poll_worker_status(port, task_id)
                if status == "success":
                    result, err = await get_worker_result(port, task_id)
                    if result:
                        completed[task_id] = (pending[task_id], result)
                    del pending[task_id]
                elif status == "failure":
                    page_num = pending[task_id]
                    logger.warning(f"[PAGE_SPLIT] Page {page_num} failed")
                    failed_pages.append({"page": page_num, "reason": "processing_failed"})
                    del pending[task_id]

            if pending:
                await asyncio.sleep(0.3)
    except Exception as e:
        logger.warning(f"[PAGE_SPLIT] Worker {port} unreachable: {e}")
        return None, f"Worker {port} unreachable: {e}"
    poll_elapsed = time.time() - poll_start

    if pending:
        logger.warning(f"[PAGE_SPLIT] Timeout waiting for {len(pending)} pages: {list(pending.values())}")
        for task_id, page_num in pending.items():
            failed_pages.append({"page": page_num, "reason": "timeout"})

    if not completed:
        return None, "No pages completed successfully"

    logger.info(f"[TIMING] Worker {port}: library_process={poll_elapsed:.3f}s for {len(completed)} pages")

    # Merge results (sorted by page number)
    merge_start = time.time()
    sorted_results = sorted(completed.values(), key=lambda x: x[0])

    # Build merged result in same format as merge_document_results expects
    sub_results = [
        {"pages": (pn, pn), "result": result}
        for pn, result in sorted_results
    ]

    merged = merge_document_results(sub_results, filename)
    merge_elapsed = time.time() - merge_start

    # Add failed_pages info and set status to "partial" if some pages failed
    if failed_pages:
        merged["failed_pages"] = sorted(failed_pages, key=lambda x: x["page"])
        merged["status"] = "partial"
        logger.warning(f"[PAGE_SPLIT] Returning partial result: {len(completed)} succeeded, {len(failed_pages)} failed")

    # Total chunk timing
    chunk_elapsed = time.time() - chunk_start_time
    logger.info(f"[TIMING] Worker {port}: merge={merge_elapsed:.3f}s, chunk_total={chunk_elapsed:.3f}s")

    # Set worker state back to ready
    worker_id = get_worker_id(port)
    logger.info(f"[PAGE_SPLIT] Setting {worker_id} (port {port}) back to ready, in_workers={worker_id in workers}")
    if worker_id in workers:
        workers[worker_id]["state"] = "ready"
        workers[worker_id]["last_activity"] = time.time()
        load_balancer.update_worker(port, workers[worker_id])
        logger.info(f"[PAGE_SPLIT] {worker_id} state now: {workers[worker_id]['state']}")
    else:
        logger.warning(f"[PAGE_SPLIT] {worker_id} not found in workers dict!")

    # merged already has {document, status, errors, processing_time, timings} format
    return merged, None


# =============================================================================
# DOCLING API ENDPOINTS (Load Balanced)
# =============================================================================

@app.post("/v1/convert/file/async")
async def convert_file_async(
    request: Request,
    files: UploadFile = File(...),
    to_formats: str = Form("json"),
    page_range: Optional[List[str]] = Form(None),
    image_export_mode: str = Form("embedded"),
    include_images: bool = Form(True),
    do_ocr: bool = Form(True),
    force_ocr: bool = Form(False),
    ocr_engine: str = Form("easyocr"),
    ocr_lang: Optional[str] = Form(None),
    do_table_structure: bool = Form(True),
    table_mode: str = Form("fast"),
    pipeline: str = Form("standard"),
    vlm_pipeline_model: Optional[str] = Form(None),
    api_key: str = Depends(verify_api_key),
):
    """
    Convert a PDF file with automatic page splitting across workers.

    Returns task_id IMMEDIATELY - processing happens in background queue.
    Workers are selected based on API key tier:
    - "windowseat" (dedicated): 8 workers (ports 5001-5008)
    - "middleseat" (shared): 2 workers (ports 5009-5010)

    Returns a job_id that can be used to poll for status and get results.
    """
    request_start = time.time()

    # Read file bytes
    file_bytes = await files.read()
    filename = files.filename or "document.pdf"
    is_pdf = filename.lower().endswith(".pdf")
    file_size = len(file_bytes)
    logger.info(f"[ASYNC] Received file: {filename}, size={file_size} bytes, is_pdf={is_pdf}, api_key={api_key}")

    # Check tier limits
    limit_error = check_file_limits(file_bytes, api_key, filename=filename, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Validate conversion parameters
    param_error = validate_conversion_params(pipeline, image_export_mode, table_mode, ocr_engine, vlm_pipeline_model)
    if param_error:
        raise HTTPException(status_code=422, detail=param_error)

    # Parse user page range if provided
    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    # For PDFs, get page count to validate and determine pages_to_process
    total_pages = 1
    pages_to_process = 1
    if is_pdf:
        try:
            total_pages = get_page_count(file_bytes)
            logger.info(f"[ASYNC] PDF has {total_pages} pages")

            if user_range:
                start_page, end_page = user_range
                start_page = max(1, min(start_page, total_pages))
                end_page = max(start_page, min(end_page, total_pages))
                pages_to_process = end_page - start_page + 1
            else:
                pages_to_process = total_pages
        except Exception as e:
            logger.error(f"[ASYNC] Invalid PDF: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    # Create job FIRST (this assigns the job_id)
    job = job_manager.create_job(
        filename=filename,
        total_pages=pages_to_process,
        user_page_range=user_range,
        api_key=api_key,
    )

    # Create job request and enqueue (non-blocking)
    job_request = JobRequest(
        job_id=job.job_id,
        api_key=api_key,
        file_bytes=file_bytes,
        filename=filename,
        is_pdf=is_pdf,
        to_formats=to_formats,
        user_range=user_range,
        total_pages=total_pages,
        image_export_mode=image_export_mode,
        include_images=include_images,
        do_ocr=do_ocr,
        force_ocr=force_ocr,
        ocr_engine=ocr_engine,
        ocr_lang=ocr_lang,
        do_table_structure=do_table_structure,
        table_mode=table_mode,
        pipeline=pipeline,
        vlm_pipeline_model=vlm_pipeline_model,
    )
    await enqueue_job(job_request)

    request_elapsed = time.time() - request_start
    logger.info(f"[ASYNC] Job {job.job_id} enqueued in {request_elapsed:.3f}s, returning immediately")

    return {
        "task_id": job.job_id,
        "status": "pending",
        "total_pages": pages_to_process,
    }


@app.post("/v1/convert/file")
async def convert_file_sync(
    request: Request,
    files: UploadFile = File(...),
    to_formats: str = Form("json"),
    page_range: Optional[List[str]] = Form(None),
    image_export_mode: str = Form("embedded"),
    include_images: bool = Form(True),
    do_ocr: bool = Form(True),
    force_ocr: bool = Form(False),
    ocr_engine: str = Form("easyocr"),
    ocr_lang: Optional[str] = Form(None),
    do_table_structure: bool = Form(True),
    table_mode: str = Form("fast"),
    pipeline: str = Form("standard"),
    vlm_pipeline_model: Optional[str] = Form(None),
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
    limit_error = check_file_limits(file_bytes, api_key, filename=filename, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Validate conversion parameters
    param_error = validate_conversion_params(pipeline, image_export_mode, table_mode, ocr_engine, vlm_pipeline_model)
    if param_error:
        raise HTTPException(status_code=422, detail=param_error)

    # Get available workers (with detailed logging)
    available_workers = await get_available_workers(api_key, max_wait=60)
    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available after 60s wait")

    # For non-PDF files, route to single coolest worker
    if not is_pdf:
        # Use load balancer to pick best worker for this api_key
        coolest_port = load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        logger.info(f"[SYNC] Non-PDF file, routing to single worker: {coolest_port}")

        # Submit to worker
        task_id, error = await submit_to_worker(coolest_port, file_bytes, filename, to_formats, image_export_mode, include_images, do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline, vlm_pipeline_model)
        if error:
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        # Poll until complete
        poll_interval = 0.5
        worker_id = get_worker_id(coolest_port)
        try:
            while True:
                status, _ = await poll_worker_status(coolest_port, task_id)
                if status == "success":
                    result, error = await get_worker_result(coolest_port, task_id)
                    # Reset worker state to ready
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        workers[worker_id]["last_activity"] = time.time()
                        load_balancer.update_worker(coolest_port, workers[worker_id])
                    if error:
                        raise HTTPException(status_code=500, detail=f"Failed to get result: {error}")
                    logger.info(f"[SYNC] Non-PDF complete")
                    return result
                elif status == "failure":
                    # Reset worker state to ready even on failure
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        load_balancer.update_worker(coolest_port, workers[worker_id])
                    raise HTTPException(status_code=500, detail="Worker task failed")
                await asyncio.sleep(poll_interval)
        except HTTPException:
            raise
        except Exception as e:
            # Reset worker state on exception
            if worker_id in workers:
                workers[worker_id]["state"] = "ready"
                load_balancer.update_worker(coolest_port, workers[worker_id])
            logger.warning(f"[SYNC] Worker {coolest_port} unreachable: {e}")
            raise HTTPException(status_code=502, detail=f"Worker {coolest_port} unreachable: {e}")

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

    # Submit chunks to workers with per-page splitting (parallel within each worker)
    async def submit_chunk_split(chunk_data: Tuple[bytes, Tuple[int, int]], port: int):
        chunk_bytes, original_pages = chunk_data
        result, error = await submit_chunk_with_page_split(
            port, chunk_bytes, original_pages, filename, to_formats, image_export_mode, include_images,
            do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline,
            vlm_pipeline_model
        )
        if error:
            logger.warning(f"[SYNC] Chunk {original_pages} failed: {error}")
            return {"pages": original_pages, "result": {"status": "failure", "errors": [{"message": error}]}}
        return {"pages": original_pages, "result": result}

    # Submit all chunks in parallel (each chunk internally splits into pages)
    logger.info(f"[SYNC] Submitting {len(chunks)} chunks with per-page splitting...")
    tasks = [submit_chunk_split(chunk, port) for chunk, port in zip(chunks, available_workers)]
    sub_results = await asyncio.gather(*tasks)

    logger.info(f"[SYNC] All {len(sub_results)} chunks completed")

    # Update worker page counts (both current life and lifetime)
    for port in available_workers[:len(chunks)]:
        worker_id = get_worker_id(port)
        if worker_id in workers:
            pages_for_worker = pages_to_process // len(chunks)
            workers[worker_id]["current_life_pages"] = workers[worker_id].get("current_life_pages", 0) + pages_for_worker
            workers[worker_id]["lifetime_pages"] = workers[worker_id].get("lifetime_pages", 0) + pages_for_worker
    merged = merge_document_results(sub_results, filename)

    logger.info(f"[SYNC] Complete: {len(sub_results)} chunks merged, status={merged['status']}")

    return merged


# =============================================================================
# CONVERT SOURCE ENDPOINTS (for uploaded files or URLs)
# =============================================================================

@app.post("/v1/convert/source/async")
async def convert_source_async(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """
    Convert a document from source (file_id from upload or URL).

    Request body should be JSON with:
    - file_id: ID returned from /v1/upload
    - OR sources: Array of URLs to fetch documents from (matches worker API)
    - to_formats: Output format (default: "json")
    - page_range: Optional [start, end] for PDF pages
    - vlm_pipeline_model: VLM model preset for vlm pipeline
    """
    body = await request.json()
    file_id = body.get("file_id")
    sources = body.get("sources")  # Array of URLs, matches worker API
    to_formats = body.get("to_formats", "json")
    page_range = body.get("page_range")
    image_export_mode = body.get("image_export_mode", "embedded")
    include_images = body.get("include_images", True)
    # OCR and table params
    do_ocr = body.get("do_ocr", True)
    force_ocr = body.get("force_ocr", False)
    ocr_engine = body.get("ocr_engine", "easyocr")
    ocr_lang = body.get("ocr_lang")
    do_table_structure = body.get("do_table_structure", True)
    table_mode = body.get("table_mode", "fast")
    pipeline = body.get("pipeline", "standard")
    vlm_pipeline_model = body.get("vlm_pipeline_model")

    if not file_id and not sources:
        raise HTTPException(status_code=400, detail="Must provide either file_id or sources array")

    # Validate conversion parameters
    param_error = validate_conversion_params(pipeline, image_export_mode, table_mode, ocr_engine, vlm_pipeline_model)
    if param_error:
        raise HTTPException(status_code=422, detail=param_error)

    if sources:
        # Proxy to worker - pass sources array directly (matches worker API)
        logger.info(f"[SOURCE] Proxying sources to worker: {sources}")

        allowed_ports = get_workers_for_api_key(api_key)
        available_workers = [
            p for p in allowed_ports
            if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
        ]

        if not available_workers:
            raise HTTPException(status_code=503, detail="No workers available")

        worker_port = load_balancer.select_single_worker(api_key)
        if not worker_port:
            worker_port = available_workers[0]

        # Pass body directly to worker (sources already in correct format)
        docling_body = {
            "sources": sources,
            "to_formats": body.get("to_formats", ["json"]),
            "image_export_mode": image_export_mode,
            "include_images": include_images,
            "do_ocr": do_ocr,
            "force_ocr": force_ocr,
            "ocr_engine": ocr_engine,
            "do_table_structure": do_table_structure,
            "table_mode": table_mode,
            "pipeline": pipeline,
        }
        if page_range:
            docling_body["page_range"] = page_range
        if ocr_lang:
            docling_body["ocr_lang"] = ocr_lang
        if vlm_pipeline_model:
            docling_body["vlm_pipeline_model"] = vlm_pipeline_model

        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.post(
                f"http://127.0.0.1:{worker_port}/v1/convert/source/async",
                json=docling_body,
            )

            if resp.status_code != 200:
                raise HTTPException(status_code=resp.status_code, detail=resp.text)

            result = resp.json()
            if "task_id" in result:
                chunk_tasks[result["task_id"]] = worker_port
            return result

    # For file_id: Get file and enqueue for processing
    request_start = time.time()

    if file_id not in uploaded_files:
        raise HTTPException(status_code=404, detail="File not found. Upload first with /v1/upload")

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

    logger.info(f"[SOURCE] Converting uploaded file {file_id}: {filename}")

    # Check tier limits
    limit_error = check_file_limits(file_bytes, api_key, filename=filename, is_pdf=is_pdf)
    if limit_error:
        raise HTTPException(status_code=413, detail=limit_error)

    # Parse user page range if provided
    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    # For PDFs, get page count to validate and determine pages_to_process
    total_pages = 1
    pages_to_process = 1
    if is_pdf:
        try:
            total_pages = get_page_count(file_bytes)
            logger.info(f"[SOURCE] PDF has {total_pages} pages")

            if user_range:
                start_page, end_page = user_range
                start_page = max(1, min(start_page, total_pages))
                end_page = max(start_page, min(end_page, total_pages))
                pages_to_process = end_page - start_page + 1
            else:
                pages_to_process = total_pages
        except Exception as e:
            logger.error(f"[SOURCE] Invalid PDF: {e}")
            raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    # Create job FIRST (this assigns the job_id)
    job = job_manager.create_job(
        filename=filename,
        total_pages=pages_to_process,
        user_page_range=user_range,
        api_key=api_key,
    )

    # Create job request and enqueue (non-blocking)
    job_request = JobRequest(
        job_id=job.job_id,
        api_key=api_key,
        file_bytes=file_bytes,
        filename=filename,
        is_pdf=is_pdf,
        to_formats=to_formats,
        user_range=user_range,
        total_pages=total_pages,
        image_export_mode=image_export_mode,
        include_images=include_images,
        do_ocr=do_ocr,
        force_ocr=force_ocr,
        ocr_engine=ocr_engine,
        ocr_lang=ocr_lang,
        do_table_structure=do_table_structure,
        table_mode=table_mode,
        pipeline=pipeline,
        vlm_pipeline_model=vlm_pipeline_model,
    )
    await enqueue_job(job_request)

    request_elapsed = time.time() - request_start
    logger.info(f"[SOURCE] Job {job.job_id} enqueued in {request_elapsed:.3f}s, returning immediately")

    return {
        "task_id": job.job_id,
        "status": "pending",
        "total_pages": pages_to_process,
    }


@app.post("/v1/convert/source")
async def convert_source_sync(
    request: Request,
    api_key: str = Depends(verify_api_key),
):
    """
    Synchronous version of /v1/convert/source/async.
    Blocks until conversion is complete and returns the result.

    Request body should be JSON with:
    - file_id: ID returned from /v1/upload
    - OR sources: Array of source objects (e.g. [{"url": "...", "kind": "http"}])
    """
    body = await request.json()
    file_id = body.get("file_id")
    sources = body.get("sources")  # Array of source objects, matches worker API
    to_formats = body.get("to_formats", "json")
    page_range = body.get("page_range")
    image_export_mode = body.get("image_export_mode", "embedded")
    include_images = body.get("include_images", True)
    # OCR and table params
    do_ocr = body.get("do_ocr", True)
    force_ocr = body.get("force_ocr", False)
    ocr_engine = body.get("ocr_engine", "easyocr")
    ocr_lang = body.get("ocr_lang")
    do_table_structure = body.get("do_table_structure", True)
    table_mode = body.get("table_mode", "fast")
    pipeline = body.get("pipeline", "standard")
    vlm_pipeline_model = body.get("vlm_pipeline_model")

    if not file_id and not sources:
        raise HTTPException(status_code=400, detail="Must provide either file_id or sources array")

    # Validate conversion parameters
    param_error = validate_conversion_params(pipeline, image_export_mode, table_mode, ocr_engine, vlm_pipeline_model)
    if param_error:
        raise HTTPException(status_code=422, detail=param_error)

    if sources:
        # Proxy to worker - pass sources array directly (matches worker API)
        allowed_ports = get_workers_for_api_key(api_key)
        available_workers = [
            p for p in allowed_ports
            if get_worker_id(p) in workers and workers[get_worker_id(p)].get("state") == "ready"
        ]

        if not available_workers:
            raise HTTPException(status_code=503, detail="No workers available")

        worker_port = load_balancer.select_single_worker(api_key)
        if not worker_port:
            worker_port = available_workers[0]

        # Pass body directly to worker (sources already in correct format)
        docling_body = {
            "sources": sources,
            "to_formats": body.get("to_formats", ["json"]),
            "image_export_mode": image_export_mode,
            "include_images": include_images,
            "do_ocr": do_ocr,
            "force_ocr": force_ocr,
            "ocr_engine": ocr_engine,
            "do_table_structure": do_table_structure,
            "table_mode": table_mode,
            "pipeline": pipeline,
        }
        if page_range:
            docling_body["page_range"] = page_range
        if ocr_lang:
            docling_body["ocr_lang"] = ocr_lang
        if vlm_pipeline_model:
            docling_body["vlm_pipeline_model"] = vlm_pipeline_model

        async with httpx.AsyncClient(timeout=600.0) as client:
            resp = await client.post(
                f"http://127.0.0.1:{worker_port}/v1/convert/source",
                json=docling_body,
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

    # Get available workers (with detailed logging)
    available_workers = await get_available_workers(api_key, max_wait=60)
    if not available_workers:
        raise HTTPException(status_code=503, detail="No workers available after 60s wait")

    # Non-PDF: single worker
    if not is_pdf:
        coolest_port = load_balancer.select_single_worker(api_key)
        if not coolest_port:
            coolest_port = available_workers[0]

        task_id, error = await submit_to_worker(coolest_port, file_bytes, filename, to_formats, image_export_mode, include_images, do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline)
        if error:
            raise HTTPException(status_code=500, detail=f"Worker error: {error}")

        worker_id = get_worker_id(coolest_port)
        try:
            while True:
                status, _ = await poll_worker_status(coolest_port, task_id)
                if status == "success":
                    result, error = await get_worker_result(coolest_port, task_id)
                    # Reset worker state to ready
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        workers[worker_id]["last_activity"] = time.time()
                        load_balancer.update_worker(coolest_port, workers[worker_id])
                    if error:
                        raise HTTPException(status_code=500, detail=f"Failed to get result: {error}")
                    return result
                elif status == "failure":
                    # Reset worker state to ready even on failure
                    if worker_id in workers:
                        workers[worker_id]["state"] = "ready"
                        load_balancer.update_worker(coolest_port, workers[worker_id])
                    raise HTTPException(status_code=500, detail="Worker task failed")
                await asyncio.sleep(0.5)
        except HTTPException:
            raise
        except Exception as e:
            # Reset worker state on exception
            if worker_id in workers:
                workers[worker_id]["state"] = "ready"
                load_balancer.update_worker(coolest_port, workers[worker_id])
            logger.warning(f"[SOURCE SYNC] Worker {coolest_port} unreachable: {e}")
            raise HTTPException(status_code=502, detail=f"Worker {coolest_port} unreachable: {e}")

    # PDF: Split and wait for all
    try:
        total_pages = get_page_count(file_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid PDF: {e}")

    user_range = None
    if page_range and len(page_range) >= 2:
        try:
            user_range = (int(page_range[0]), int(page_range[1]))
        except (ValueError, IndexError):
            pass

    try:
        chunks = split_pdf_for_workers(file_bytes, len(available_workers), user_range)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error splitting PDF: {e}")

    # Submit all chunks
    async def submit_chunk(chunk_data, port):
        chunk_bytes, original_pages = chunk_data
        task_id, error = await submit_to_worker(port, chunk_bytes, filename, to_formats, image_export_mode, include_images, do_ocr, force_ocr, ocr_engine, ocr_lang, do_table_structure, table_mode, pipeline)
        if error:
            return None
        return (port, task_id, original_pages)

    tasks = [submit_chunk(chunk, port) for chunk, port in zip(chunks, available_workers)]
    results = await asyncio.gather(*tasks)
    task_info = [r for r in results if r is not None]

    if not task_info:
        raise HTTPException(status_code=500, detail="Failed to submit to any worker")

    # Update worker page counts
    pages_to_process = sum(chunk[1][1] - chunk[1][0] + 1 for chunk in chunks)
    for port in available_workers[:len(chunks)]:
        worker_id = get_worker_id(port)
        if worker_id in workers:
            pages_for_worker = pages_to_process // len(chunks)
            workers[worker_id]["current_life_pages"] = workers[worker_id].get("current_life_pages", 0) + pages_for_worker
            workers[worker_id]["lifetime_pages"] = workers[worker_id].get("lifetime_pages", 0) + pages_for_worker

    # Poll until all complete
    max_wait = 600
    start_time = time.time()
    completed_results = {}

    # Helper to reset worker state
    def reset_worker_state(port):
        worker_id = get_worker_id(port)
        if worker_id in workers:
            workers[worker_id]["state"] = "ready"
            workers[worker_id]["last_activity"] = time.time()
            load_balancer.update_worker(port, workers[worker_id])

    try:
        while len(completed_results) < len(task_info):
            if time.time() - start_time > max_wait:
                # Reset all worker states on timeout
                for port, _, _ in task_info:
                    reset_worker_state(port)
                raise HTTPException(status_code=504, detail=f"Conversion timed out after {max_wait}s")

            for port, task_id, page_range_chunk in task_info:
                if task_id in completed_results:
                    continue

                status, _ = await poll_worker_status(port, task_id)

                if status == "success":
                    result, error = await get_worker_result(port, task_id)
                    # Reset worker state to ready
                    reset_worker_state(port)
                    if result:
                        completed_results[task_id] = {"pages": page_range_chunk, "result": result}
                    else:
                        completed_results[task_id] = {"pages": page_range_chunk, "result": {"status": "failure", "errors": [{"message": error}]}}
                elif status == "failure":
                    # Reset worker state to ready even on failure
                    reset_worker_state(port)
                    completed_results[task_id] = {"pages": page_range_chunk, "result": {"status": "failure", "errors": [{"message": "Worker reported failure"}]}}

            if len(completed_results) < len(task_info):
                await asyncio.sleep(0.5)
    except HTTPException:
        raise
    except Exception as e:
        # Reset all worker states on exception
        for port, _, _ in task_info:
            reset_worker_state(port)
        logger.warning(f"[SOURCE SYNC] Worker unreachable during polling: {e}")
        raise HTTPException(status_code=502, detail=f"Worker unreachable: {e}")

    # Merge results
    sub_results = list(completed_results.values())
    return merge_document_results(sub_results, filename)


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
    merged = merge_document_results(sub_results, job.filename)

    # Preserve summed worker time in timings, show wall clock as processing_time
    if job.started_at and job.completed_at:
        if "timings" not in merged:
            merged["timings"] = {}
        merged["timings"]["total_worker_time"] = merged["processing_time"]  # preserve summed time
        merged["processing_time"] = job.completed_at - job.started_at  # wall clock for client

    return merged


@app.get("/v1/status/poll/{task_id}")
async def poll_status(task_id: str, api_key: str = Depends(verify_api_key)):
    """
    Poll job status.

    For split jobs, aggregates status from all sub-jobs.
    For chunk tasks, proxies to the worker.
    """
    # Check if this is a chunk task - proxy to worker
    if task_id in chunk_tasks:
        worker_port = chunk_tasks[task_id]
        try:
            status, _ = await poll_worker_status(worker_port, task_id)
        except Exception as e:
            logger.warning(f"[POLL] Worker {worker_port} unreachable for chunk task {task_id}: {e}")
            status = "failure"
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

    # Check ownership - job must belong to this API key
    if job.api_key and job.api_key != api_key:
        raise HTTPException(status_code=403, detail="Access denied")

    # Use job.status as single source of truth
    task_status = job.status.value  # "pending", "started", "success", "failure"

    # Save results to disk when completed (if not already saved)
    if job.status == JobStatus.SUCCESS and not result_storage.result_exists(task_id):
        merged = _build_merged_result(job)
        result_storage.save_result(task_id, merged)
        result_mb = len(str(merged)) / (1024 * 1024)
        duration_so_far = time.time() - job.started_at if job.started_at else 0
        logger.info(f"[JOB {task_id}] COMPLETE: {job.total_pages} pages, {len(job.sub_jobs)} workers, {duration_so_far:.1f}s, result={result_mb:.1f}MB")

    # Calculate duration
    duration = None
    if job.started_at:
        end_time = job.completed_at if job.completed_at else time.time()
        duration = round(end_time - job.started_at, 2)

    return {
        "task_id": task_id,
        "task_type": "convert",
        "task_status": task_status,
        "task_position": None,
        "task_meta": None,
        "error": job.error,
        "received_at": epoch_to_utc(job.received_at),
        "started_at": epoch_to_utc(job.started_at),
        "completed_at": epoch_to_utc(job.completed_at),
        "duration_seconds": duration,
    }


@app.get("/v1/result/{task_id}")
async def get_result(task_id: str, verbose: bool = False, api_key: str = Depends(verify_api_key)):
    """
    Get job result in standard docling-serve format.

    Args:
        task_id: The job/task ID
        verbose: If true, include detailed per-page timing breakdown

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
            merged = merge_document_results(disk_result["sub_results"], disk_result.get("filename", "document.pdf"))
            # Update disk with new format
            result_storage.save_result(task_id, merged)
            disk_result = merged

        # Save detailed timings if not already saved
        timings_data = disk_result.get("timings")
        existing_timings = result_storage.get_job_timings(task_id)
        logger.info(f"[TIMINGS] task={task_id}, has_timings={bool(timings_data)}, existing={bool(existing_timings)}")
        if timings_data and not existing_timings:
            save_result = result_storage.save_job_timings(task_id, timings_data)
            logger.info(f"[TIMINGS] Saved timings for {task_id}: {save_result}")

        # Strip detailed timings unless verbose
        if not verbose and "timings" in disk_result:
            disk_result = {k: v for k, v in disk_result.items() if k != "timings"}
        return disk_result

    job = job_manager.get_job(task_id)

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Check ownership - job must belong to this API key
    if job.api_key and job.api_key != api_key:
        raise HTTPException(status_code=403, detail="Access denied")

    if job.status != JobStatus.SUCCESS:
        raise HTTPException(status_code=400, detail=f"Job not complete: {job.status.value}")

    # Build merged result in standard docling-serve format
    merged = _build_merged_result(job)

    # Save detailed timings to stats
    if merged.get("timings"):
        result_storage.save_job_timings(task_id, merged["timings"])

    # Save to disk for persistence
    result_storage.save_result(task_id, merged)

    # Strip detailed timings unless verbose
    if not verbose and "timings" in merged:
        merged = {k: v for k, v in merged.items() if k != "timings"}

    return merged


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


# =============================================================================
# DOCS PASSTHROUGH
# =============================================================================

@app.get("/docs", include_in_schema=False)
async def docs_page():
    """Serve Swagger UI pointing to worker's OpenAPI spec."""
    from fastapi.openapi.docs import get_swagger_ui_html
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Docling API")


@app.get("/redoc", include_in_schema=False)
async def redoc_page():
    """Serve ReDoc pointing to worker's OpenAPI spec."""
    from fastapi.openapi.docs import get_redoc_html
    return get_redoc_html(openapi_url="/openapi.json", title="Docling API")


@app.get("/openapi.json", include_in_schema=False)
async def openapi_proxy():
    """Proxy OpenAPI spec from worker for rich documentation."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"http://{WORKER_HOST}:{MIN_PORT}/openapi.json")
            if resp.status_code == 200:
                return Response(content=resp.content, media_type="application/json")
    except Exception as e:
        logger.warning(f"Failed to fetch worker OpenAPI: {e}")
    # Fallback: return minimal spec
    return {"openapi": "3.0.0", "info": {"title": "Docling API", "version": "1.0.0"}, "paths": {}}


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
                stored_pid = worker["pid"]
                port = worker["port"]
                logger.info(f"[WATCHDOG] Processing {worker_id}: port={port}, state={worker['state']}, life_pages={worker.get('current_life_pages', 0)}, lifetime={worker.get('lifetime_pages', 0)}, restarts={worker.get('restart_count', 0)}, restart_pending={worker.get('restart_pending', False)}")

                # Get the REAL PID from lsof - this is the source of truth
                real_pid = get_real_pid_on_port(port)
                logger.info(f"[WATCHDOG] {worker_id}: stored_pid={stored_pid}, real_pid={real_pid}")

                # If no process on port, worker is dead
                if real_pid is None:
                    logger.info(f"[WATCHDOG] {worker_id}: NO PROCESS ON PORT {port}! Triggering restart...")
                    print(f"Watchdog: {worker_id} no process on port {port}, restarting...")
                    await restart_worker(worker_id, reason="process_died")
                    continue

                # If PID changed (e.g., external restart), update our tracking
                if real_pid != stored_pid:
                    logger.warning(f"[WATCHDOG] {worker_id}: PID MISMATCH! stored={stored_pid}, real={real_pid}. Updating...")
                    worker["pid"] = real_pid
                    stored_pid = real_pid

                # Use real_pid for all subsequent checks
                pid = real_pid

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

                # Check for failed state (set when submit_to_worker fails)
                if worker["state"] == "failed":
                    logger.info(f"[WATCHDOG] {worker_id}: state=failed, triggering restart...")
                    await restart_worker(worker_id, reason="submit_failed")
                    continue

                # Check restart_pending tag (set during job assignment when page limit reached)
                # TAG-based restart: worker was tagged for restart, now waiting for it to be idle
                if worker.get("restart_pending"):
                    current_life_pages = worker.get("current_life_pages", 0)
                    if worker["state"] == "ready":
                        logger.info(f"[WATCHDOG] {worker_id}: restart_pending=True, state=ready, triggering restart (pages={current_life_pages})...")
                        await restart_worker(worker_id, reason=f"page_limit ({current_life_pages} pages)")
                        continue
                    else:
                        logger.info(f"[WATCHDOG] {worker_id}: restart_pending=True but state={worker['state']}, waiting for ready...")

                # Check idle timeout (also respects MAX_UNAVAILABLE)
                idle_time = time.time() - worker["last_activity"]
                if idle_time > IDLE_RESTART_SEC and worker["state"] == "ready" and not worker.get("restart_pending"):
                    # Count unavailable workers (restarting or pending restart)
                    unavailable_count = sum(
                        1 for w in workers.values()
                        if w.get("restart_pending") or w.get("state") == "restarting"
                    )
                    if unavailable_count < MAX_UNAVAILABLE:
                        logger.info(f"[WATCHDOG] {worker_id}: idle for {idle_time:.0f}s > {IDLE_RESTART_SEC}s, restarting... (unavailable={unavailable_count}/{MAX_UNAVAILABLE})")
                        await restart_worker(worker_id, reason="idle_timeout")
                        continue
                    else:
                        logger.info(f"[WATCHDOG] {worker_id}: deferring idle restart, {unavailable_count} workers already unavailable")

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
