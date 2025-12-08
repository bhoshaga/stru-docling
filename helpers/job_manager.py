"""
Job management for tracking jobs, sub-jobs, and metrics.
"""
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any
from threading import Lock

logger = logging.getLogger(__name__)


class JobStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SubJob:
    """A sub-job representing a chunk of pages sent to a worker."""
    sub_job_id: str
    worker_port: int
    original_pages: tuple  # (start, end) in original PDF
    worker_task_id: Optional[str] = None  # task_id returned by worker
    status: JobStatus = JobStatus.PENDING
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Dict] = None
    error: Optional[str] = None

    # Per-page metrics (filled after completion)
    page_metrics: Dict[int, Dict] = field(default_factory=dict)


@dataclass
class Job:
    """A parent job that may be split across multiple workers."""
    job_id: str
    api_key: Optional[str] = None  # Creator's API key for access control
    status: JobStatus = JobStatus.PENDING
    received_at: float = field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None

    # PDF info
    filename: Optional[str] = None
    total_pages: int = 0
    user_page_range: Optional[tuple] = None  # User requested range

    # Sub-jobs (one per worker chunk)
    sub_jobs: List[SubJob] = field(default_factory=list)

    # Aggregated result
    result: Optional[Dict] = None
    error: Optional[str] = None

    # Metrics
    metrics: Dict = field(default_factory=dict)

    def to_dict(self) -> Dict:
        """Convert to dictionary for API response."""
        return {
            "job_id": self.job_id,
            "status": self.status.value,
            "received_at": self.received_at,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "filename": self.filename,
            "total_pages": self.total_pages,
            "user_page_range": self.user_page_range,
            "sub_jobs": [
                {
                    "sub_job_id": sj.sub_job_id,
                    "worker_port": sj.worker_port,
                    "original_pages": sj.original_pages,
                    "status": sj.status.value,
                    "started_at": sj.started_at,
                    "completed_at": sj.completed_at,
                    "error": sj.error,
                }
                for sj in self.sub_jobs
            ],
            "error": self.error,
            "metrics": self.metrics,
        }


class JobManager:
    """
    Manages job tracking in memory with thread-safe operations.
    """

    def __init__(self):
        self._jobs: Dict[str, Job] = {}
        self._sub_job_to_job: Dict[str, str] = {}  # sub_job_id -> job_id
        self._worker_task_to_sub_job: Dict[str, str] = {}  # worker_task_id -> sub_job_id
        self._lock = Lock()

        # Aggregate stats
        self.stats = {
            "jobs_processed": 0,
            "jobs_failed": 0,
            "pages_processed": 0,
            "total_processing_time": 0.0,
        }

    def create_job(
        self,
        filename: Optional[str] = None,
        total_pages: int = 0,
        user_page_range: Optional[tuple] = None,
        api_key: Optional[str] = None,
    ) -> Job:
        """Create a new parent job."""
        job_id = str(uuid.uuid4())
        job = Job(
            job_id=job_id,
            api_key=api_key,
            filename=filename,
            total_pages=total_pages,
            user_page_range=user_page_range,
        )

        with self._lock:
            self._jobs[job_id] = job

        return job

    def add_sub_job(
        self,
        job_id: str,
        worker_port: int,
        original_pages: tuple,
    ) -> SubJob:
        """Add a sub-job to a parent job."""
        sub_job_id = str(uuid.uuid4())
        sub_job = SubJob(
            sub_job_id=sub_job_id,
            worker_port=worker_port,
            original_pages=original_pages,
        )

        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].sub_jobs.append(sub_job)
                self._sub_job_to_job[sub_job_id] = job_id

        return sub_job

    def start_job(self, job_id: str):
        """Mark a job as started."""
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].status = JobStatus.IN_PROGRESS
                self._jobs[job_id].started_at = time.time()

    def register_worker_task(self, sub_job_id: str, worker_task_id: str):
        """Register the worker's task_id for a sub-job."""
        with self._lock:
            job_id = self._sub_job_to_job.get(sub_job_id)
            if job_id and job_id in self._jobs:
                for sj in self._jobs[job_id].sub_jobs:
                    if sj.sub_job_id == sub_job_id:
                        sj.worker_task_id = worker_task_id
                        sj.status = JobStatus.IN_PROGRESS
                        sj.started_at = time.time()
                        self._worker_task_to_sub_job[worker_task_id] = sub_job_id
                        break

    def complete_sub_job(
        self,
        sub_job_id: str,
        result: Optional[Dict] = None,
        error: Optional[str] = None,
        page_metrics: Optional[Dict] = None,
    ):
        """Mark a sub-job as completed or failed."""
        with self._lock:
            job_id = self._sub_job_to_job.get(sub_job_id)
            if not job_id or job_id not in self._jobs:
                return

            job = self._jobs[job_id]
            for sj in job.sub_jobs:
                if sj.sub_job_id == sub_job_id:
                    sj.completed_at = time.time()
                    if error:
                        sj.status = JobStatus.FAILED
                        sj.error = error
                    else:
                        sj.status = JobStatus.COMPLETED
                        sj.result = result
                        if page_metrics:
                            sj.page_metrics = page_metrics
                    break

            # Check if all sub-jobs are done
            self._check_job_completion(job)

    def _check_job_completion(self, job: Job):
        """Check if all sub-jobs are complete and update parent job."""
        if not job.sub_jobs:
            return

        all_done = all(
            sj.status in (JobStatus.COMPLETED, JobStatus.FAILED)
            for sj in job.sub_jobs
        )

        if not all_done:
            return

        job.completed_at = time.time()

        # Check for failures
        failed = [sj for sj in job.sub_jobs if sj.status == JobStatus.FAILED]
        if failed:
            job.status = JobStatus.FAILED
            job.error = f"{len(failed)} sub-job(s) failed"
            self.stats["jobs_failed"] += 1
        else:
            job.status = JobStatus.COMPLETED
            self.stats["jobs_processed"] += 1

            # Aggregate metrics
            total_time = job.completed_at - job.started_at if job.started_at else 0
            pages = sum(
                sj.original_pages[1] - sj.original_pages[0] + 1
                for sj in job.sub_jobs
            )
            self.stats["pages_processed"] += pages
            self.stats["total_processing_time"] += total_time

            # Collect page metrics
            job.metrics = {
                "wall_time": total_time,
                "pages": {},
            }
            for sj in job.sub_jobs:
                for page_num, metrics in sj.page_metrics.items():
                    job.metrics["pages"][page_num] = metrics

    def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        with self._lock:
            return self._jobs.get(job_id)

    def get_sub_job_by_worker_task(self, worker_task_id: str) -> Optional[tuple]:
        """Get (job_id, sub_job) by worker's task_id."""
        with self._lock:
            sub_job_id = self._worker_task_to_sub_job.get(worker_task_id)
            if not sub_job_id:
                return None

            job_id = self._sub_job_to_job.get(sub_job_id)
            if not job_id or job_id not in self._jobs:
                return None

            job = self._jobs[job_id]
            for sj in job.sub_jobs:
                if sj.sub_job_id == sub_job_id:
                    return (job_id, sj)

            return None

    def get_worker_for_job(self, job_id: str) -> Optional[int]:
        """Get the worker port for a single-chunk job."""
        with self._lock:
            job = self._jobs.get(job_id)
            if job and len(job.sub_jobs) == 1:
                return job.sub_jobs[0].worker_port
            return None

    def get_pending_jobs_for_worker(self, worker_port: int) -> List[str]:
        """Get job IDs with pending results on a worker."""
        with self._lock:
            pending = []
            for job_id, job in self._jobs.items():
                for sj in job.sub_jobs:
                    if sj.worker_port == worker_port and sj.status == JobStatus.IN_PROGRESS:
                        pending.append(job_id)
                        break
            return pending

    def get_pending_sub_jobs_for_worker(self, worker_port: int) -> List[tuple]:
        """Get (job_id, sub_job) tuples for pending sub-jobs on a worker."""
        with self._lock:
            logger.info(f"[DEBUG] get_pending_sub_jobs_for_worker(port={worker_port})")
            logger.info(f"[DEBUG]   Total jobs in _jobs: {len(self._jobs)}")
            pending = []
            for job_id, job in self._jobs.items():
                logger.info(f"[DEBUG]   Job {job_id}: status={job.status.value}, sub_jobs={len(job.sub_jobs)}")
                for sj in job.sub_jobs:
                    logger.info(f"[DEBUG]     SubJob {sj.sub_job_id}: port={sj.worker_port}, status={sj.status.value}, task_id={sj.worker_task_id}")
                    if sj.worker_port == worker_port and sj.status == JobStatus.IN_PROGRESS:
                        pending.append((job_id, sj))
                        logger.info(f"[DEBUG]       -> MATCHED! Added to pending")
            logger.info(f"[DEBUG]   Returning {len(pending)} pending sub-jobs for port {worker_port}")
            return pending

    def get_stats(self) -> Dict:
        """Get aggregate statistics."""
        with self._lock:
            avg_time = 0
            if self.stats["pages_processed"] > 0:
                avg_time = self.stats["total_processing_time"] / self.stats["pages_processed"]

            return {
                "jobs_processed": self.stats["jobs_processed"],
                "jobs_failed": self.stats["jobs_failed"],
                "pages_processed": self.stats["pages_processed"],
                "avg_time_per_page": round(avg_time, 2),
                "active_jobs": sum(
                    1 for j in self._jobs.values()
                    if j.status == JobStatus.IN_PROGRESS
                ),
                "pending_jobs": sum(
                    1 for j in self._jobs.values()
                    if j.status == JobStatus.PENDING
                ),
            }

    def cleanup_old_jobs(self, max_age_seconds: int = 3600):
        """Remove jobs older than max_age_seconds."""
        now = time.time()
        with self._lock:
            to_remove = []
            for job_id, job in self._jobs.items():
                if job.status in (JobStatus.COMPLETED, JobStatus.FAILED):
                    age = now - (job.completed_at or job.received_at)
                    if age > max_age_seconds:
                        to_remove.append(job_id)

            for job_id in to_remove:
                job = self._jobs.pop(job_id, None)
                if job:
                    for sj in job.sub_jobs:
                        self._sub_job_to_job.pop(sj.sub_job_id, None)
                        if sj.worker_task_id:
                            self._worker_task_to_sub_job.pop(sj.worker_task_id, None)

            return len(to_remove)

    def timeout_stuck_jobs(self, timeout_seconds: int = 600) -> int:
        """Mark jobs stuck in IN_PROGRESS for too long as FAILED."""
        now = time.time()
        timed_out = 0
        with self._lock:
            for job_id, job in self._jobs.items():
                if job.status == JobStatus.IN_PROGRESS:
                    started = job.started_at or job.received_at
                    elapsed = now - started
                    if elapsed > timeout_seconds:
                        job.status = JobStatus.FAILED
                        job.completed_at = now
                        job.error = f"Job timed out after {elapsed:.0f}s (limit: {timeout_seconds}s)"
                        self.stats["jobs_failed"] += 1
                        timed_out += 1
                        logger.warning(f"Job {job_id} timed out after {elapsed:.0f}s")

                        # Also mark any stuck sub-jobs as failed
                        for sj in job.sub_jobs:
                            if sj.status == JobStatus.IN_PROGRESS:
                                sj.status = JobStatus.FAILED
                                sj.completed_at = now
                                sj.error = "Timed out"

        return timed_out


# Global instance
job_manager = JobManager()
