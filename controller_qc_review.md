# Controller QC Review

## Architecture Snapshot
- `controller.py` is the FastAPI entrypoint: API-key auth, sync/async convert endpoints, URL/file-id conversion, chunk proxies, worker admin, and watchdog/cleanup loops. It tracks worker state in the global `workers` dict and persists job/worker stats via `helpers/result_storage.py`.
- Helpers: PDF splitting (`helpers/pdf_utils.py`), in-memory job tracking with per-job API key stored (`helpers/job_manager.py`), load balancing (`helpers/load_balancer.py`), async job queue for the async endpoints (`helpers/job_queue.py`), result merging (`helpers/result_merger.py`), upload dedupe (`helpers/upload_router.py`), stats router, chunk proxy router, and worker admin router. Dependencies are injected at startup to avoid circular imports.
- API keys define tiers and allowed worker ports; background tasks persist stats/results to disk and periodically clean old files/jobs.

## Critical Issues
- **Disk-stored results bypass ownership checks.** When a job ages out of memory (cleanup_task removes jobs after ~1h) or the controller restarts, `get_result` returns the on-disk result without verifying the caller’s API key (`controller.py:1799-1821`). Any valid key plus a leaked UUID can download another tenant’s document. Similarly, `poll_status` returns “completed” for a disk-only job without an ownership check (`controller.py:1733-1740`), leaking job existence/status across tenants.
- **Chunk-task results are unscoped.** For chunk endpoints, the task-to-worker mapping (`chunk_tasks`) stores no API key, and both status/result paths only require any valid key (`controller.py:1714-1729`, `controller.py:1789-1797`). A guessed task_id would let another tenant poll or fetch chunk outputs.
