"""
Helper modules for the Docling controller.
"""
from .pdf_utils import (
    get_page_count,
    get_page_sizes,
    extract_pages,
    split_page_ranges,
    split_pdf_for_workers,
)
from .job_manager import (
    JobStatus,
    SubJob,
    Job,
    JobManager,
    job_manager,
)
from .load_balancer import (
    LoadBalancer,
    load_balancer,
)
from .result_storage import (
    ResultStorage,
    result_storage,
)
from .upload_router import (
    router as upload_router,
    init_router as init_upload_router,
    cleanup_old_uploads,
    uploaded_files,
)
from .stats_router import (
    router as stats_router,
    init_router as init_stats_router,
)
from .result_merger import (
    merge_document_results,
)
from .chunk_router import (
    router as chunk_router,
    init_router as init_chunk_router,
)
from .worker_router import (
    router as worker_router,
    init_router as init_worker_router,
)
from .retry_handler import (
    submit_chunk_with_retry,
)
from .file_validation import (
    validate_file_type,
    is_supported_file,
    SUPPORTED_EXTENSIONS,
)
from .job_queue import (
    JobRequest,
    init_queue as init_job_queue,
    enqueue_job,
    job_queue_consumer,
    get_queue_stats,
)

__all__ = [
    # PDF utils
    "get_page_count",
    "get_page_sizes",
    "extract_pages",
    "split_page_ranges",
    "split_pdf_for_workers",
    # Job manager
    "JobStatus",
    "SubJob",
    "Job",
    "JobManager",
    "job_manager",
    # Load balancer
    "LoadBalancer",
    "load_balancer",
    # Result storage
    "ResultStorage",
    "result_storage",
    # Upload router
    "upload_router",
    "init_upload_router",
    "cleanup_old_uploads",
    "uploaded_files",
    # Stats router
    "stats_router",
    "init_stats_router",
    # Result merger
    "merge_document_results",
    # Chunk router
    "chunk_router",
    "init_chunk_router",
    # Worker router
    "worker_router",
    "init_worker_router",
    # Retry handler
    "submit_chunk_with_retry",
    # File validation
    "validate_file_type",
    "is_supported_file",
    "SUPPORTED_EXTENSIONS",
    # Job queue
    "JobRequest",
    "init_job_queue",
    "enqueue_job",
    "job_queue_consumer",
    "get_queue_stats",
]
