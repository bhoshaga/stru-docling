# Controller QC Review

## Architecture Snapshot
- `controller.py` hosts the FastAPI app, tiered API-key auth, conversion endpoints (sync/async), worker lifecycle endpoints, and background watchdog/cleanup loops. Workers are external `docling-serve` processes on ports 5001-5010 tracked in the global `workers` map.
- Work distribution relies on `helpers/load_balancer.py` (per-key pools) and in-memory job tracking in `helpers/job_manager.py`, with disk persistence for results/stats via `helpers/result_storage.py`.
- PDF utilities for splitting/counting live in `helpers/pdf_utils.py`; uploads and stats are exposed via dependency-injected routers (`helpers/upload_router.py`, `helpers/stats_router.py`).

## Critical Findings
- Async conversion routes block until all work is done. `convert_file_async` and `convert_source_async` wait for every chunk/page to finish before responding (`controller.py:966`, `controller.py:1440`), so large docs hold the HTTP connection for minutes and are likely to hit client/proxy timeouts. The API still returns a “pending” task_id, but the work has already completed synchronously—defeating async behavior and risking duplicate processing on retries.
- Worker state leaks after sync conversions. `submit_to_worker` marks a worker `processing` (`controller.py:573`), but sync flows that use it never flip back to `ready`: non-PDF `/v1/convert/file` (`controller.py:1172`), source sync non-PDF (`controller.py:1537`), and source sync PDF chunking (`controller.py:1572`). Each call permanently removes the worker from the ready pool, so a handful of sync requests can exhaust all workers until a manual restart.
- No admin separation on management APIs. Worker lifecycle endpoints `/worker/add`, `/worker/{id}/restart`, `/worker/{id}/remove` only require any valid tier API key (`controller.py:2063`, `controller.py:2083`, `controller.py:2093`), and stats endpoints share the same auth (`helpers/stats_router.py:22`). Any customer key can spawn/kill workers or view operational details, which is a service-takeover risk.
