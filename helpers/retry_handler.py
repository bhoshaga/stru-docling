"""
Retry handler for failed chunk submissions.

Handles automatic retry with wait-for-worker logic when a worker fails mid-job.
"""

import asyncio
import logging
import time
from typing import Callable, Dict, List, Optional, Tuple, Awaitable

logger = logging.getLogger(__name__)

# Configuration
RETRY_WAIT_TIMEOUT = 60  # seconds to wait for a worker to become ready


async def submit_chunk_with_retry(
    port: int,
    chunk_bytes: bytes,
    original_pages: Tuple[int, int],
    filename: str,
    to_formats: str,
    api_key: str,
    job_id: str,
    # Dependencies injected from controller
    submit_func: Callable[..., Awaitable[Tuple[Optional[dict], Optional[str]]]],
    get_workers_for_api_key_func: Callable[[str], List[int]],
    get_worker_id_func: Callable[[int], str],
    workers_dict: Dict[str, dict],
) -> Tuple[Optional[dict], Optional[str]]:
    """
    Submit chunk with automatic retry on failure.

    If initial submission fails, waits for another worker to become ready
    (up to RETRY_WAIT_TIMEOUT seconds) and retries once.

    Args:
        port: Initial worker port to try
        chunk_bytes: PDF chunk bytes
        original_pages: (start_page, end_page) tuple
        filename: Original filename
        to_formats: Output format
        api_key: API key for worker pool selection
        job_id: Parent job ID for logging
        submit_func: Function to submit chunk (submit_chunk_with_page_split)
        get_workers_for_api_key_func: Function to get allowed ports for API key
        get_worker_id_func: Function to get worker ID from port
        workers_dict: Reference to workers state dict

    Returns:
        (result, error) - result dict if successful, error string if failed
    """
    # Initial submission attempt
    result, error = await submit_func(
        port, chunk_bytes, original_pages, filename, to_formats
    )

    if error:
        logger.warning(f"[JOB {job_id}] Worker {port} failed for pages {original_pages[0]}-{original_pages[1]}: {error}")

        # Find another available worker with WAIT LOOP
        allowed_ports = get_workers_for_api_key_func(api_key)
        retry_port = None
        retry_wait_start = time.time()

        logger.info(f"[JOB {job_id}] RETRY_WAIT: Looking for available worker (timeout={RETRY_WAIT_TIMEOUT}s)...")

        while time.time() - retry_wait_start < RETRY_WAIT_TIMEOUT:
            # Check all allowed workers for one that's ready
            for p in allowed_ports:
                if p == port:
                    continue  # Skip the failed worker
                worker_id = get_worker_id_func(p)
                if worker_id in workers_dict:
                    w = workers_dict[worker_id]
                    state = w.get("state", "unknown")
                    restart_pending = w.get("restart_pending", False)

                    if state == "ready" and not restart_pending:
                        retry_port = p
                        elapsed = time.time() - retry_wait_start
                        logger.info(f"[JOB {job_id}] RETRY_WAIT: Found ready worker {retry_port} after {elapsed:.1f}s")
                        break

            if retry_port:
                break

            # Log what we're waiting for
            elapsed = time.time() - retry_wait_start
            worker_states = []
            for p in allowed_ports:
                if p == port:
                    continue
                wid = get_worker_id_func(p)
                if wid in workers_dict:
                    w = workers_dict[wid]
                    worker_states.append(f"{wid}:{w.get('state', '?')}")
            logger.info(f"[JOB {job_id}] RETRY_WAIT: No ready workers, waiting... ({elapsed:.0f}s) states=[{', '.join(worker_states)}]")

            await asyncio.sleep(2)

        if not retry_port:
            elapsed = time.time() - retry_wait_start
            logger.error(f"[JOB {job_id}] RETRY_WAIT: TIMEOUT after {elapsed:.1f}s - no workers became ready")

        if retry_port:
            logger.info(f"[JOB {job_id}] RETRY: Submitting pages {original_pages[0]}-{original_pages[1]} to worker {retry_port}")
            result, error = await submit_func(
                retry_port, chunk_bytes, original_pages, filename, to_formats
            )
            if error:
                logger.error(f"[JOB {job_id}] RETRY: Worker {retry_port} also failed: {error}")
            else:
                logger.info(f"[JOB {job_id}] RETRY: SUCCESS on worker {retry_port}")

        if error:
            logger.error(f"[JOB {job_id}] FAILED pages {original_pages[0]}-{original_pages[1]} after retry: {error}")

    return result, error
