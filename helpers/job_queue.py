"""
Async job queue for truly non-blocking job submission.

Jobs are added to the queue immediately and processed by a background consumer
that waits for available workers before splitting and submitting work.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from .job_manager import JobStatus

logger = logging.getLogger(__name__)


@dataclass
class JobRequest:
    """All information needed to process a job."""
    job_id: str
    api_key: str
    file_bytes: bytes
    filename: str
    is_pdf: bool
    to_formats: str
    user_range: Optional[Tuple[int, int]] = None
    total_pages: int = 0
    image_export_mode: str = "embedded"  # embedded, placeholder, referenced
    include_images: bool = True
    # OCR and table params
    do_ocr: bool = True
    force_ocr: bool = False
    ocr_engine: str = "easyocr"
    ocr_lang: Optional[str] = None
    do_table_structure: bool = True
    table_mode: str = "fast"
    pipeline: str = "standard"
    enqueued_at: float = field(default_factory=time.time)


# The job queue
_job_queue: asyncio.Queue[JobRequest] = None

# Dependencies injected from controller
_get_available_workers: Callable = None
_split_pdf_for_workers: Callable = None
_submit_chunk_with_retry: Callable = None
_submit_chunk_with_page_split: Callable = None
_submit_to_worker: Callable = None
_poll_worker_status: Callable = None
_get_worker_result: Callable = None
_get_workers_for_api_key: Callable = None
_get_worker_id: Callable = None
_workers: Dict = None
_job_manager: Any = None
_load_balancer: Any = None
_MAX_PAGES_BEFORE_RESTART: int = 10


def init_queue(
    get_available_workers_func: Callable,
    split_pdf_for_workers_func: Callable,
    submit_chunk_with_retry_func: Callable,
    submit_chunk_with_page_split_func: Callable,
    submit_to_worker_func: Callable,
    poll_worker_status_func: Callable,
    get_worker_result_func: Callable,
    get_workers_for_api_key_func: Callable,
    get_worker_id_func: Callable,
    workers_dict: Dict,
    job_manager_instance: Any,
    load_balancer_instance: Any,
    max_pages_before_restart: int,
):
    """Initialize queue with dependencies from controller."""
    global _job_queue, _get_available_workers, _split_pdf_for_workers
    global _submit_chunk_with_retry, _submit_chunk_with_page_split, _submit_to_worker
    global _poll_worker_status, _get_worker_result
    global _get_workers_for_api_key, _get_worker_id, _workers
    global _job_manager, _load_balancer, _MAX_PAGES_BEFORE_RESTART

    _job_queue = asyncio.Queue()
    _get_available_workers = get_available_workers_func
    _split_pdf_for_workers = split_pdf_for_workers_func
    _submit_chunk_with_retry = submit_chunk_with_retry_func
    _submit_chunk_with_page_split = submit_chunk_with_page_split_func
    _submit_to_worker = submit_to_worker_func
    _poll_worker_status = poll_worker_status_func
    _get_worker_result = get_worker_result_func
    _get_workers_for_api_key = get_workers_for_api_key_func
    _get_worker_id = get_worker_id_func
    _workers = workers_dict
    _job_manager = job_manager_instance
    _load_balancer = load_balancer_instance
    _MAX_PAGES_BEFORE_RESTART = max_pages_before_restart

    logger.info("[QUEUE] Job queue initialized")


async def enqueue_job(request: JobRequest) -> None:
    """Add a job to the queue for processing."""
    await _job_queue.put(request)
    queue_size = _job_queue.qsize()
    logger.info(f"[QUEUE] Enqueued job {request.job_id}: {request.filename}, queue_size={queue_size}")


async def job_queue_consumer():
    """
    Background task that processes jobs from the queue.

    Runs forever, pulling jobs and waiting for workers before processing.
    This is where the blocking happens - NOT in the endpoint.
    """
    logger.info("[QUEUE] Consumer started, waiting for jobs...")

    while True:
        try:
            # Block until a job is available
            request = await _job_queue.get()
            queue_wait = time.time() - request.enqueued_at
            logger.info(f"[QUEUE] Dequeued job {request.job_id}: {request.filename}, waited {queue_wait:.3f}s in queue")

            try:
                await _process_job(request)
            except Exception as e:
                logger.error(f"[QUEUE] Failed to process job {request.job_id}: {e}")
                # Mark job as failed
                job = _job_manager.get_job(request.job_id)
                if job:
                    job.status = JobStatus.FAILED
                    job.error = str(e)
                    job.completed_at = time.time()
            finally:
                _job_queue.task_done()

        except Exception as e:
            logger.error(f"[QUEUE] Consumer error: {e}")
            await asyncio.sleep(1)  # Prevent tight loop on errors


async def _process_job(request: JobRequest):
    """Process a single job from the queue."""
    job_id = request.job_id

    # Wait for available workers (this is where blocking happens)
    logger.info(f"[QUEUE] Job {job_id}: waiting for workers...")
    wait_start = time.time()
    available_workers = await _get_available_workers(request.api_key, max_wait=60)
    wait_elapsed = time.time() - wait_start

    if not available_workers:
        logger.error(f"[QUEUE] Job {job_id}: no workers available after 60s")
        raise Exception("No workers available after 60s wait")

    logger.info(f"[QUEUE] Job {job_id}: got {len(available_workers)} workers after {wait_elapsed:.3f}s wait")

    # Get the job from job_manager
    job = _job_manager.get_job(job_id)
    if not job:
        logger.error(f"[QUEUE] Job {job_id}: job not found in job_manager")
        raise Exception("Job not found")

    # Non-PDF: route to single worker
    if not request.is_pdf:
        await _process_non_pdf(request, available_workers)
        return

    # PDF: split across workers
    await _process_pdf(request, available_workers)


async def _process_non_pdf(request: JobRequest, available_workers: List[int]):
    """Process a non-PDF file on a single worker."""
    job_id = request.job_id

    # Pick the least busy worker
    coolest_port = _load_balancer.select_single_worker(request.api_key)
    if not coolest_port:
        coolest_port = available_workers[0]

    logger.info(f"[QUEUE] Job {job_id}: non-PDF, routing to worker {coolest_port}")

    # Start the job
    _job_manager.start_job(job_id)

    # Create sub-job
    sub_job = _job_manager.add_sub_job(
        job_id=job_id,
        worker_port=coolest_port,
        original_pages=(1, 1),
    )

    # Submit to worker
    task_id, error = await _submit_to_worker(
        coolest_port, request.file_bytes, request.filename, request.to_formats,
        request.image_export_mode, request.include_images,
        request.do_ocr, request.force_ocr, request.ocr_engine, request.ocr_lang,
        request.do_table_structure, request.table_mode, request.pipeline
    )

    if error:
        logger.error(f"[QUEUE] Job {job_id}: worker submit error: {error}")
        _job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
        return

    _job_manager.register_worker_task(sub_job.sub_job_id, task_id)
    logger.info(f"[QUEUE] Job {job_id}: submitted to worker {coolest_port}, task_id={task_id}")

    # Poll worker until complete
    while True:
        try:
            status, _ = await _poll_worker_status(coolest_port, task_id)
            if status in ("success", "failure"):
                break
        except Exception as e:
            logger.error(f"[QUEUE] Job {job_id}: poll error: {e}")
            _job_manager.complete_sub_job(sub_job.sub_job_id, error=f"Poll error: {e}")
            # Reset worker state on error
            worker_id = _get_worker_id(coolest_port)
            if worker_id in _workers:
                _workers[worker_id]["state"] = "ready"
                _load_balancer.update_worker(coolest_port, _workers[worker_id])
            return
        await asyncio.sleep(0.5)

    # Get result and complete sub_job
    if status == "success":
        result, get_error = await _get_worker_result(coolest_port, task_id)
        if get_error:
            logger.error(f"[QUEUE] Job {job_id}: get_result error: {get_error}")
            _job_manager.complete_sub_job(sub_job.sub_job_id, error=get_error)
        else:
            logger.info(f"[QUEUE] Job {job_id}: non-PDF completed on worker {coolest_port}")
            _job_manager.complete_sub_job(sub_job.sub_job_id, result=result)
    else:
        logger.error(f"[QUEUE] Job {job_id}: worker reported failure")
        _job_manager.complete_sub_job(sub_job.sub_job_id, error="Worker reported failure")

    # Reset worker state to ready
    worker_id = _get_worker_id(coolest_port)
    if worker_id in _workers:
        _workers[worker_id]["state"] = "ready"
        _workers[worker_id]["last_activity"] = time.time()
        _load_balancer.update_worker(coolest_port, _workers[worker_id])
        logger.info(f"[QUEUE] Job {job_id}: reset {worker_id} state to ready")


async def _process_pdf(request: JobRequest, available_workers: List[int]):
    """Process a PDF by splitting across workers."""
    job_id = request.job_id

    # Split PDF across workers
    split_start = time.time()
    chunks = _split_pdf_for_workers(request.file_bytes, len(available_workers), request.user_range)
    split_elapsed = time.time() - split_start

    logger.info(f"[QUEUE] Job {job_id}: split into {len(chunks)} chunks in {split_elapsed:.3f}s")
    for i, (chunk_bytes, page_range) in enumerate(chunks):
        worker_port = available_workers[i]
        chunk_mb = len(chunk_bytes) / (1024 * 1024)
        pages = page_range[1] - page_range[0] + 1
        logger.info(f"[QUEUE] Job {job_id}:   Chunk {i+1}: pages {page_range[0]}-{page_range[1]} ({pages} pages, {chunk_mb:.1f}MB) -> worker {worker_port}")

    # Start the job
    _job_manager.start_job(job_id)

    # Define chunk submission function
    async def submit_chunk(chunk_data: Tuple[bytes, Tuple[int, int]], port: int):
        chunk_bytes, original_pages = chunk_data

        sub_job = _job_manager.add_sub_job(
            job_id=job_id,
            worker_port=port,
            original_pages=original_pages,
        )

        result, error = await _submit_chunk_with_retry(
            port=port,
            chunk_bytes=chunk_bytes,
            original_pages=original_pages,
            filename=request.filename,
            to_formats=request.to_formats,
            api_key=request.api_key,
            job_id=job_id,
            submit_func=_submit_chunk_with_page_split,
            get_workers_for_api_key_func=_get_workers_for_api_key,
            get_worker_id_func=_get_worker_id,
            workers_dict=_workers,
            image_export_mode=request.image_export_mode,
            include_images=request.include_images,
            do_ocr=request.do_ocr,
            force_ocr=request.force_ocr,
            ocr_engine=request.ocr_engine,
            ocr_lang=request.ocr_lang,
            do_table_structure=request.do_table_structure,
            table_mode=request.table_mode,
            pipeline=request.pipeline,
        )

        if error:
            _job_manager.complete_sub_job(sub_job.sub_job_id, error=error)
            logger.error(f"[QUEUE] Job {job_id}: worker {port} failed pages {original_pages[0]}-{original_pages[1]}: {error}")
            return False

        _job_manager.complete_sub_job(sub_job.sub_job_id, result=result)
        logger.info(f"[QUEUE] Job {job_id}: worker {port} completed pages {original_pages[0]}-{original_pages[1]}")
        return True

    # Update worker page counts and spawn background tasks
    for i, (chunk, port) in enumerate(zip(chunks, available_workers)):
        chunk_bytes, page_range = chunk
        pages_for_worker = page_range[1] - page_range[0] + 1
        worker_id = _get_worker_id(port)

        if worker_id in _workers:
            _workers[worker_id]["current_life_pages"] = _workers[worker_id].get("current_life_pages", 0) + pages_for_worker
            _workers[worker_id]["lifetime_pages"] = _workers[worker_id].get("lifetime_pages", 0) + pages_for_worker

            # Don't tag for restart here - let tag_workers_for_restart() handle it
            # (respects MAX_UNAVAILABLE limit)

            _load_balancer.update_worker(port, _workers[worker_id])
            logger.info(f"[QUEUE] Job {job_id}: assigned {pages_for_worker} pages to {worker_id}, life_pages={_workers[worker_id]['current_life_pages']}")

        # Spawn background task
        asyncio.create_task(submit_chunk(chunk, port))

    logger.info(f"[QUEUE] Job {job_id}: spawned {len(chunks)} background tasks")


def get_queue_stats() -> Dict:
    """Get queue statistics."""
    if _job_queue is None:
        return {"initialized": False}

    return {
        "initialized": True,
        "queue_size": _job_queue.qsize(),
    }
