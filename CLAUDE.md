# Claude Code Instructions for stru-docling

## CRITICAL: No Assumptions

**DO NOT use "probably", "maybe", "perhaps", "I think", "should be".**

Always verify with actual commands. Back up every statement with data and facts. If you don't know, CHECK IT - don't guess.

You will be fucked in the ass if you don't check and give unhelpful answers if you violate this rule. Depending on how badly you fuckup, prepare to be fucked in the ass.

---

## SSH Access to io.net GPU Server

```bash
ssh -p 28988 ionet@3.21.229.114
```

---

## CRITICAL: Always Enable CUDA

The server has an **RTX 4090**. Always start docling-serve with GPU enabled:

```bash
export DOCLING_DEVICE=cuda
docling-serve run --host 0.0.0.0 --port 5001
```

Or in one command:
```bash
ssh -p 28988 ionet@3.21.229.114 "export PATH=/opt/conda/bin:/home/ionet/.local/bin:\$PATH && export DOCLING_DEVICE=cuda && nohup docling-serve run --host 0.0.0.0 --port 5001 > ~/server.log 2>&1 &"
```

**Environment variables:**
- `DOCLING_DEVICE=cuda` - Use GPU (RTX 4090)
- `DOCLING_NUM_THREADS=19` - Use all 19 CPU cores (default is only 4!)
- `OMP_NUM_THREADS=19` - OpenMP threads for C++ parts

Full startup command:
```bash
export DOCLING_DEVICE=cuda
export DOCLING_NUM_THREADS=19
export OMP_NUM_THREADS=19
docling-serve run --host 0.0.0.0 --port 5001
```

**DO NOT run with defaults** - you'll only use 4 threads and no GPU.

To check SSH keys on GitHub account:
```bash
gh ssh-key list
```

Note: `gh secret list` shows secret names but values are write-only (cannot be read).

---

## CRITICAL: Running Commands on Remote Server

When asked to run tests or check things on the server, you must SSH into the machine. Do NOT run commands locally.

```bash
# CORRECT - run command on server
ssh -p 28988 ionet@3.21.229.114 "curl -s http://localhost:5001/health"

# WRONG - this runs locally, not on server
curl -s http://localhost:5001/health
```

### Check if a file exists ON THE SERVER (not locally!)
```bash
ssh -p 28988 ionet@3.21.229.114 "ls -la ~/sample-drawing.pdf"
```

### Upload a file to the server
```bash
scp -v -P 28988 /local/path/file.pdf ionet@3.21.229.114:~/
```

**ALWAYS use `-v` flag with scp** to see transfer progress.

---

## CRITICAL: Always Use Async API

The sync endpoint `/v1/convert/file` returns **404 with misleading error** on large PDFs:
```json
{"detail":"Task result not found. Please wait for a completion status."}
```

This is NOT a missing file - it's an internal timeout. **ALWAYS use the async endpoint.**

### Async Conversion Pattern

```python
import requests
import time

url = "http://localhost:5001"

def convert_pdf_async(pdf_path, page_range=None):
    """
    Convert PDF using async API. NEVER use /v1/convert/file (sync).

    Args:
        pdf_path: Path to PDF on the SERVER (not local machine)
        page_range: Optional tuple (start, end) for page range
    """
    # Step 1: Submit async job
    data = [("to_formats", "md")]  # Only specify format, use all other defaults
    if page_range:
        # page_range MUST be sent as two separate form fields
        data.append(("page_range", str(page_range[0])))
        data.append(("page_range", str(page_range[1])))

    with open(pdf_path, "rb") as f:
        r = requests.post(
            f"{url}/v1/convert/file/async",  # ASYNC endpoint!
            files={"files": f},
            data=data
        )
    task_id = r.json()["task_id"]

    # Step 2: Poll until complete
    while True:
        resp = requests.get(f"{url}/v1/status/poll/{task_id}")
        status = resp.json()["task_status"]
        if status == "success":
            return requests.get(f"{url}/v1/result/{task_id}").json()
        elif status == "failure":
            raise Exception(f"Task failed: {task_id}")
        time.sleep(0.5)
```

### Async Endpoints
| Endpoint | Use |
|----------|-----|
| `POST /v1/convert/file/async` | Submit conversion job |
| `GET /v1/status/poll/{task_id}` | Check job status |
| `GET /v1/result/{task_id}` | Get result (only after status=success) |

---

## CRITICAL: Use Full Default Settings for Benchmarks

Do NOT disable features when benchmarking. The server defaults are:
- `do_ocr=True` (EasyOCR)
- `do_table_structure=True`
- `image_export_mode=embedded`
- `pipeline=standard`

```python
# CORRECT - let server use defaults
data = [("to_formats", "md")]

# WRONG - don't disable features, this gives unrealistic numbers
data = [
    ("to_formats", "md"),
    ("do_ocr", "false"),
    ("do_table_structure", "false"),
]
```

---

## CRITICAL: page_range Format

The `page_range` parameter is an array `[start, end]`. In multipart form data, arrays must be sent as **multiple fields with the same name**.

### Python requests
```python
# CORRECT
data = [
    ("to_formats", "md"),
    ("page_range", "1"),   # First element
    ("page_range", "5"),   # Second element
]
requests.post(url, files={"files": f}, data=data)

# WRONG - all of these will be IGNORED silently (processes ALL pages)
data = {"page_range": "1-5"}
data = {"page_range": "1,5"}
data = {"page_range": [1, 5]}
data = [("page_range", "1-5")]
```

### curl
```bash
# CORRECT
curl -X POST http://localhost:5001/v1/convert/file/async \
  -F "files=@file.pdf" \
  -F "to_formats=md" \
  -F "page_range=1" \
  -F "page_range=5"

# WRONG
curl ... -F "page_range=1-5"
curl ... -F "page_range=1,5"
```

---

## Complete Benchmark Script

Run this ON THE SERVER to test GIL patch speedup:

```bash
ssh -p 28988 ionet@3.21.229.114 'python3 << "EOF"
import requests
import time
from concurrent.futures import ThreadPoolExecutor

pdf_path = "/home/ionet/sample-drawing.pdf"  # Must exist on server!
url = "http://localhost:5001"

def convert_async(page_range=None):
    data = [("to_formats", "md")]
    if page_range:
        data.append(("page_range", str(page_range[0])))
        data.append(("page_range", str(page_range[1])))

    with open(pdf_path, "rb") as f:
        r = requests.post(f"{url}/v1/convert/file/async", files={"files": f}, data=data)
    task_id = r.json()["task_id"]

    while True:
        status = requests.get(f"{url}/v1/status/poll/{task_id}").json()["task_status"]
        if status in ("success", "failure"):
            return status
        time.sleep(0.5)

# Test 1: Sequential (baseline)
print("Sequential (all pages)...")
t0 = time.time()
status = convert_async()
t_seq = time.time() - t0
print(f"  Status: {status}, Time: {t_seq:.2f}s")

# Test 2: Parallel (2 requests)
print("Parallel (2 requests)...")
t0 = time.time()
ranges = [(1, 5), (6, 10)]
with ThreadPoolExecutor(max_workers=2) as ex:
    results = list(ex.map(convert_async, ranges))
t_parallel = time.time() - t0
print(f"  Status: {results}, Time: {t_parallel:.2f}s")
print(f"  Speedup: {t_seq/t_parallel:.2f}x (expect ~1.7x)")
EOF'
```

---

## Server Management

### Check server status
```bash
ssh -p 28988 ionet@3.21.229.114 "ps aux | grep docling-serve"
ssh -p 28988 ionet@3.21.229.114 "curl -s http://localhost:5001/health"
```

### View logs (check here when things fail!)
```bash
ssh -p 28988 ionet@3.21.229.114 "tail -100 ~/server.log"
```

### Restart server
```bash
ssh -p 28988 ionet@3.21.229.114 "pkill -f 'docling-serve'; sleep 2"
ssh -p 28988 ionet@3.21.229.114 "export PATH=/opt/conda/bin:/home/ionet/.local/bin:\$PATH && nohup docling-serve run --host 0.0.0.0 --port 5001 > ~/server.log 2>&1 &"
```

### Verify patched docling-parse
```bash
ssh -p 28988 ionet@3.21.229.114 "/opt/conda/bin/pip list | grep docling-parse"
# Expected: docling-parse 4.7.2 /home/ionet/stru-docling/docling-parse-patched
```

---

## Current Benchmark Results (Dec 3, 2025)

**io.net server, full defaults** (OCR + tables + embedded images, 10-page 24MB PDF):

| Test | Time | Speedup |
|------|------|---------|
| Sequential | 101.90s | baseline |
| Parallel (2 req) | 60.15s | **1.69x** |

---

## Troubleshooting

### "no existing pdf_resources_dir" Error

If you see this error in the logs:
```
RuntimeError: no existing pdf_resources_dir: /home/ionet/.local/lib/python3.13/site-packages/docling_parse/pdf_resources_v2/
```

This means the server is looking for resources in the old site-packages path instead of the patched editable install. The folder **does exist** in the correct location:
```
/home/ionet/stru-docling/docling-parse-patched/docling_parse/pdf_resources_v2/
```

**Fix: Restart the server.** It cached the old path from before the patch was installed.

```bash
ssh -p 28988 ionet@3.21.229.114 "pkill -f 'docling-serve'; sleep 2"
ssh -p 28988 ionet@3.21.229.114 "export PATH=/opt/conda/bin:/home/ionet/.local/bin:\$PATH && nohup docling-serve run --host 0.0.0.0 --port 5001 > ~/server.log 2>&1 &"
```

---

## Docker Build

**Always use buildx** (legacy `docker build` is deprecated):

```bash
docker buildx build -t docling-serve-patched:latest .
```

**NOT** `docker build` - it will show deprecation warning and may be removed.

---

## CRITICAL: Always Use PDF Backend V4

**ALWAYS use `pdf_backend=dlparse_v4`** (the default). NEVER use V2.

V2 has a severe memory leak - backends accumulate memory and never release it. See:
- https://github.com/docling-project/docling/issues/2209
- https://github.com/docling-project/docling-serve/issues/366

```python
# CORRECT - V4 is default, but be explicit
data = [
    ("to_formats", "md"),
    ("pdf_backend", "dlparse_v4"),
]

# WRONG - V2 leaks memory catastrophically
data = [
    ("to_formats", "md"),
    ("pdf_backend", "dlparse_v2"),  # DO NOT USE
]
```

---

## CRITICAL: Logging - server.log is the Single Source of Truth

**ALL logs go to `./server.log`** in the project root. This is the single source of truth for debugging.

### Log File Location
- **Default**: `./server.log` (in project root)
- **Override**: Set `LOG_FILE` environment variable

### What Gets Logged

The controller logs **everything** with structured prefixes:

```
[UPLOAD] - File upload events (received, saved, duplicates, cleanup)
[JOB <job_id>] - Job lifecycle (created, split, worker completion, final status)
[SYNC] - Synchronous conversion events
[SOURCE] - Source/URL conversion events
[CHUNK] - Chunk endpoint proxying
[RESTART] - Worker restart events
[WATCHDOG] - Background monitoring
```

### Example Log Output

```
[UPLOAD] Received file: sample.pdf, size=24612388 bytes, hash=a1b2c3d4e5f6..., api_key=windowseat
[UPLOAD] Saved file as abc123-def456.pdf (original: sample.pdf, hash=a1b2c3d4e5f6...)
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f] Created from file_id: sample.pdf, 10 pages, 2 workers available
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f] SPLIT: 2 chunks across 2 workers
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f]   Chunk 1: pages 1-5 (5 pages, 12.2MB) -> worker 5001
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f]   Chunk 2: pages 6-10 (5 pages, 11.3MB) -> worker 5002
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f] Worker 5001 completed pages 1-5
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f] Worker 5002 completed pages 6-10
[JOB e8bc2a7c-ea0b-4229-8d7f-eecdf0087a3f] COMPLETE: 10 pages, 2 workers, 67.9s, result=24.1MB
```

### Viewing Logs

```bash
# Watch logs in real-time
tail -f ./server.log

# Search for specific job
grep "\[JOB abc123" ./server.log

# See all uploads
grep "\[UPLOAD\]" ./server.log

# See all job completions
grep "COMPLETE:" ./server.log
```

### Workers Can Use This Logger

All components (controller, workers, helpers) should use the same logger:
```python
import logging
logger = logging.getLogger(__name__)
logger.info("[PREFIX] Your message here")
```

---

## Controller Architecture

The controller (`controller.py`) manages multiple docling-serve workers with:

### Key Features
- **Load balancing**: Distributes work across workers
- **Page splitting**: Large PDFs split across multiple workers for parallel processing
- **Tier-based limits**: Different limits for different API keys
- **Deduplication**: SHA256 hash prevents duplicate uploads
- **Job tracking**: Full lifecycle tracking with timing

### API Key Tiers

| Tier | API Key | Workers | Max File | Max Pages |
|------|---------|---------|----------|-----------|
| Dedicated | `windowseat` | 5001-5008 | 200MB | 400 |
| Shared | `middleseat` | 5009-5010 | 20MB | 20 |

### Endpoints

| Category | Endpoint | Description |
|----------|----------|-------------|
| Upload | `POST /v1/upload` | Upload file, get file_id (with dedup) |
| Convert | `POST /v1/convert/file/async` | Convert uploaded file |
| Convert | `POST /v1/convert/source/async` | Convert by file_id or URL |
| Status | `GET /v1/status/poll/{task_id}` | Poll job status |
| Result | `GET /v1/result/{task_id}` | Get conversion result |
| Stats | `GET /stats` | Processing statistics |
| Health | `GET /health` | Health check (no auth) |
| Version | `GET /version` | Version info (no auth) |

### Upload Deduplication

Files are deduplicated using SHA256 hash:
- Same file uploaded twice → returns existing `file_id`
- Response includes `"duplicate": true` if already exists
- Saves disk space and processing time

---

## CRITICAL: Restarting the Controller Properly

**The controller MUST be restarted from the project directory** for code changes to take effect.

### Proper Restart Procedure

```bash
# 1. Kill the old controller process
pkill -9 -f "python.*controller"

# 2. Wait for it to die
sleep 2

# 3. Verify it's dead
pgrep -f "controller.py" || echo "Controller killed"

# 4. Start new controller FROM THE PROJECT DIRECTORY
cd /Users/bhoshaga/stru-docling  # or your project path
nohup python3 controller.py > /tmp/ctrl_out.log 2>&1 &

# 5. Wait for startup
sleep 4

# 6. Verify it's running with new code
curl -s http://localhost:8000/health
```

### Common Mistakes
- **Not killing old process** → Old code keeps running
- **Process still alive after pkill** → Use `kill -9 <pid>` directly
- **Starting from wrong directory** → Imports fail or old code runs
- **Not waiting for startup** → Health check fails

### Verify New Code is Running
Check logs for expected output format. If logs look different from what code should produce, the old process is still running.

---

## CRITICAL: Starting Workers and Controller (Local Development)

**The controller does NOT auto-spawn docling-serve workers.** You must start them manually.

### Full Startup Procedure

```bash
# 1. Kill any existing processes
pkill -9 -f "python.*controller"
pkill -9 -f "docling-serve"
sleep 2

# 2. Start docling-serve workers (at least 2 for parallel processing)
nohup docling-serve run --host 0.0.0.0 --port 5001 > /tmp/worker1.log 2>&1 &
nohup docling-serve run --host 0.0.0.0 --port 5002 > /tmp/worker2.log 2>&1 &

# 3. Wait for workers to initialize (~8 seconds)
sleep 8

# 4. Verify workers are running
curl -s http://localhost:5001/health && curl -s http://localhost:5002/health
# Should output: {"status":"ok"}{"status":"ok"}

# 5. Start the controller (it will find the running workers)
cd /Users/bhoshaga/stru-docling
nohup python3 controller.py > /tmp/ctrl_out.log 2>&1 &

# 6. Wait and verify
sleep 5
curl -s http://localhost:8000/health
# Should output: {"status":"ok"}

# 7. Verify controller found workers
grep "Found.*workers" server.log | tail -1
# Should show: Found 2 workers: ['docling_1', 'docling_2']
```

### Common Mistakes
- **Starting controller without workers** → "Found 0 workers" → jobs hang waiting for workers
- **Workers died but controller still running** → Jobs hang, need to restart workers then controller
- **Multiple controller processes** → Old code handles requests, check with `pgrep -a controller`

---

## Worker Restart Configuration

Workers automatically restart based on these limits (set via environment variables):

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_PAGES_BEFORE_RESTART` | 10 | Restart worker after processing N pages |
| `MAX_MEMORY_MB` | 12000 | Restart worker if memory exceeds N MB |
| `IDLE_RESTART_SEC` | 3600 | Restart idle worker after N seconds |
| `JOB_TIMEOUT_SEC` | 600 | Mark job as failed after N seconds |

---

## Mistakes Claude Will Make (Avoid These)

1. **Not enabling CUDA** → Always set `DOCLING_DEVICE=cuda` before starting server. The RTX 4090 will sit idle otherwise.
2. **Using sync API** → Returns 404 timeout error. Always use `/v1/convert/file/async`
3. **Using legacy docker build** → Use `docker buildx build` instead
4. **Running commands locally** → Must SSH into server first
5. **Not checking if PDF exists on server** → Check with `ssh ... "ls ~/file.pdf"`
6. **Disabling OCR/tables** → Use full defaults for realistic benchmarks
7. **Wrong page_range format** → Must be two separate form fields, not "1-5" or [1,5]
8. **Not polling async tasks** → Must poll `/v1/status/poll/{task_id}` until success/failure
9. **Not checking logs on error** → `tail ./server.log` shows actual errors
10. **Using pdf_backend V2** → V2 has catastrophic memory leaks. Always use V4 (dlparse_v4)
11. **Truncating IDs in logs** → Always log full UUIDs, never truncate with `[:8]`
12. **Not restarting controller after code changes** → Controller.py changes require restart. ALWAYS kill and restart the controller after editing controller.py. Verify new code is running by checking logs for new log messages/formats.
13. **Using too-short timeouts in tests** → PDF conversion takes 20-40s. Use at least 120s timeout for HTTP requests in test scripts.
14. **Starting controller without checking port 8000** → ALWAYS run `lsof -i :8000` before starting controller. If port is in use, kill the old process first. Old controllers running old code will silently handle requests.
15. **Not clearing Python cache** → ALWAYS run `rm -rf __pycache__ helpers/__pycache__` before restarting controller after code changes. Python may use stale .pyc bytecode files.
16. **Not verifying new code is running** → After restart, check `server.log` for expected new log messages. If logs look different from what code should produce, old process is still running or cache wasn't cleared.
17. **Not tracing actual code path** → When adding functionality, trace the ACTUAL code path from endpoint to execution. Don't assume a function is being called - VERIFY IT. If debug logs don't appear, the code path is wrong. Check for inline/duplicate implementations that bypass your changes.

### Proper Controller Restart Procedure

**IMPORTANT:** Don't use `pkill -f "python.*controller"` - it doesn't match properly on macOS.
Instead, kill by PORT which is reliable:

```bash
# 1. Kill process on port 8000 (reliable method)
lsof -ti :8000 | xargs kill -9 2>/dev/null
sleep 2

# 2. Verify port is free
lsof -i :8000  # Must show NOTHING

# 3. Clear Python cache
rm -rf __pycache__ helpers/__pycache__

# 4. Clear logs for clean slate (optional)
rm -f server.log

# 5. Start fresh controller
cd /Users/bhoshaga/stru-docling
nohup python3 controller.py > /tmp/ctrl_out.log 2>&1 &

# 6. Wait and verify
sleep 4
curl -s http://localhost:8000/health
```

**For workers too:**
```bash
lsof -ti :5001 | xargs kill -9 2>/dev/null
lsof -ti :5002 | xargs kill -9 2>/dev/null
```
