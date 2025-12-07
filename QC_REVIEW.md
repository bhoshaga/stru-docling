# QC Review (v2)

## Architecture (re-read)
- `controller.py` hosts the FastAPI app, wiring API key auth, async/sync conversion endpoints, chunking proxies, worker management endpoints, and background watchdog/cleanup loops. Worker state lives in the global `workers` dict and is mirrored into `helpers/load_balancer.py`.
- Jobs/sub-jobs are tracked in-memory via `helpers/job_manager.py` (thread-lock guarded) and persisted to disk with `helpers/result_storage.py`; PDF splitting lives in `helpers/pdf_utils.py`. Upload + source conversion routes are factored into `helpers/upload_router.py` and are dependency-injected from the controller at startup.

## Critical Issues
- **Tier isolation is porous across multiple routes.**
  - Worker selection for non-PDF and chunk routes uses `load_balancer.select_single_worker` without intersecting with the caller’s allowed ports (controller.py:607-639, 1265-1400; helpers/upload_router.py:190-240, 345-402). Because `LoadBalancer` candidates include both shared and dedicated pools by default, a shared-tier API key can be routed to dedicated workers.
  - The legacy direct proxy `/{port}/{path}` only checks that the API key is valid, not that the port is permitted for that key (controller.py:1649-1687). Any valid key can call a dedicated worker directly by port number.
  - No dedicated pool assignment is configured in `helpers/load_balancer.py`, so even if the code intended isolation, the current configuration exposes all workers to all keys.
- **Watchdog restarts can kill active work.**
  - Worker `state` never transitions to `"processing"` and `last_activity` is only set at startup/restart (controller.py:145, 331, 422). `update_worker_state` is defined but never called. The idle restart check (`idle_time > IDLE_RESTART_SEC and state == "ready"`) will therefore restart any “ready” worker after the timeout even if it is actively handling tasks. Result: in-flight jobs can be terminated and lost when the watchdog or idle policy triggers.
- **Tier limits are bypassed on chunk routes.**
  - `/v1/chunk/...` (file and source, sync and async) never calls `check_file_limits`, so users can send oversized files/page counts regardless of tier limits (controller.py:1265-1400). Upload/source routes in `helpers/upload_router.py` likewise proxy URL sources without size/page checks. This defeats the tier enforcement present on the main `/v1/convert/file` endpoints.

## Secondary Risks (not “critical” but worth noting)
- Poll/result mapping for chunk tasks (`chunk_tasks`) is only kept in memory; after a controller restart, clients cannot retrieve results for in-flight chunk jobs.
- Background cleanup deletes uploads older than an hour without regard to whether a client might intend to reuse `file_id` later; may be intentional but is a UX/data-availability caveat.
