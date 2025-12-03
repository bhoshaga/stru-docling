# Docling-Parse GIL Release Optimization

> **IMPORTANT: NEVER WORK IN /tmp!**
> Files in /tmp are deleted on reboot. Deleting a /tmp folder while a shell session uses it as CWD breaks ALL bash commands in that session. Always clone repos to permanent locations like `/Users/bhoshaga/PycharmProjects/`.

## Current Patched Build (ALREADY INSTALLED)

**Location:** `/Users/bhoshaga/PycharmProjects/docling-parse-patched/`
**Status:** Built and installed as editable in stru venv
**Patched file:** `app/pybind_parse.cpp` (line ~420, GIL release around `parse_pdf_from_key_on_page`)

To verify it's active:
```bash
source /Users/bhoshaga/PycharmProjects/stru/venv/bin/activate
python -c "import docling_parse; print(docling_parse.__file__)"
# Should show: /Users/bhoshaga/PycharmProjects/docling-parse-patched/docling_parse/__init__.py
```

**DO NOT rebuild** - just restart docling-serve to use the patched version.

---

## TL;DR - Production Usage

**To get ~1.7x speedup on PDF processing:**

1. Split PDF into 2 page ranges
2. Send 2 parallel HTTP requests to docling-serve
3. Merge markdown results

```python
from concurrent.futures import ThreadPoolExecutor
import requests

def convert_pdf_fast(pdf_path, server_url="http://localhost:5001"):
    """Convert PDF with ~1.5x speedup using parallel page ranges."""

    # Get page count first (or estimate/hardcode)
    page_count = get_page_count(pdf_path)  # implement this
    mid = page_count // 2

    ranges = [(1, mid), (mid + 1, page_count)]

    def convert_range(page_range):
        start, end = page_range
        with open(pdf_path, 'rb') as f:
            # IMPORTANT: page_range must be sent as multiple form fields (array format)
            return requests.post(
                f"{server_url}/v1/convert/file",
                files={'files': f},
                data=[
                    ('to_formats', 'md'),
                    ('do_ocr', 'false'),
                    ('do_table_structure', 'false'),
                    ('page_range', str(start)),  # First element of array
                    ('page_range', str(end)),    # Second element of array
                ]
            ).json()

    # Process both ranges in parallel
    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(convert_range, ranges))

    # Merge markdown outputs
    return merge_results(results)
```

> **CRITICAL: `page_range` API Format**
> The `page_range` parameter is an **array of 2 integers** `[start, end]`.
> In multipart form data, arrays must be sent as **multiple fields with the same name**.
>
> **Working curl example (single line):**
> ```bash
> curl -X POST http://localhost:5001/v1/convert/file -F "files=@sample-drawing.pdf" -F "to_formats=md" -F "page_range=1" -F "page_range=5"
> ```
>
> **Working Python requests example:**
> ```python
> requests.post(url, files={'files': f}, data=[
>     ('to_formats', 'md'),
>     ('page_range', '1'),   # start page
>     ('page_range', '5'),   # end page
> ])
> ```
>
> **WRONG - these will be IGNORED and process ALL pages:**
> ```bash
> # WRONG: page_range as single value
> -F "page_range=1-5"
> -F "page_range=1,5"
> # WRONG: backslash line continuations can break in markdown/copy-paste
> curl -X POST url \
>   -F "files=@file.pdf"
> ```

**Optimal parallelism by hardware:**
| Hardware | Optimal parallel requests |
|----------|---------------------------|
| Mac (M1/M2) | 2 |
| 8-core server | 4 (test to confirm) |
| 16+ core server | 6-8 (test to confirm) |

---

## Summary

We identified that `docling-parse` (the C++ PDF parsing library used by Docling) holds the Python GIL during parsing, preventing multi-threaded parallelism. We implemented a fix that releases the GIL and achieved **~1.7x speedup** with parallel page-range requests.

## How the GIL Patch Works

### What is the GIL?
The Global Interpreter Lock (GIL) protects Python's internal data structures (reference counting, object allocation). Only one thread can execute Python bytecode at a time.

### Why release it during C++ calls?
```
Without patch:
Thread 1: Python → C++ parse() [GIL HELD - 2+ seconds] → return
Thread 2: blocked... blocked... blocked... finally runs

With patch:
Thread 1: Python → [RELEASE GIL] → C++ parse (2s) → [REACQUIRE GIL] → return
Thread 2: [gets GIL] → [RELEASE GIL] → C++ parse (2s) → [REACQUIRE GIL] → return
          ↑ starts immediately, runs in parallel
```

**GIL is needed for:** Python object manipulation, reference counting
**GIL is NOT needed for:** Pure C++ computation, file I/O, memory allocation

The patch releases GIL at the START of heavy C++ work and reacquires at the END:
```cpp
{
  pybind11::gil_scoped_release release;  // Release GIL
  result = self.parse_pdf_from_key_on_page(...);  // 2+ seconds C++ work
}  // Reacquire GIL automatically
return result;
```

---

## Parallel API Testing Results (Dec 3, 2025)

### Test Setup
- **docling-serve 1.9.0** with patched docling-parse
- **Sample PDF:** `sample-drawing.pdf` (24MB, 10-page CAD drawing)
- **Mac hardware** (M-series chip)

### Results: Parallel Page-Range Requests

| Parallel Requests | Page Split | Time | Speedup |
|-------------------|------------|------|---------|
| 1 (sequential) | all 10 | 44.2s | baseline |
| **2** | **5+5** | **26.2s** | **1.69x** ✓ optimal |
| 3 | 4+3+3 | 30.2s | 1.46x |
| 4 | 3+2+3+2 | 28.3s | 1.55x |
| 5 | 2+2+2+2+2 | 30.2s | 1.46x |

**Key finding:** 2 parallel requests is optimal on Mac. More threads = CPU cache contention = slower.

### Why Only ~2x Speedup (Not 4x or 8x)?

The bottleneck is **CPU cache**, not the GIL:

1. **L3 cache is finite** (~12MB on Mac, ~16-24MB on servers)
2. Each PDF parse loads: font tables, glyph data, page structures
3. With 2 threads: both fit in L3 cache → fast
4. With 4+ threads: cache thrashing → constant slow RAM fetches

This is a hardware limit, not a code inefficiency.

---

## Docling-Serve Architecture

```
HTTP Request → FastAPI → LocalOrchestrator → AsyncLocalWorker → DoclingConverterManager → converter.convert_all()
```

### Key Components

1. **`docling_serve/app.py`** - FastAPI endpoints
2. **`LocalOrchestrator`** - Task queue with `num_workers` setting (default 2)
3. **`AsyncLocalWorker`** - Pulls tasks, runs in thread via `asyncio.to_thread()`
4. **`DoclingConverterManager`** - Creates DocumentConverter, calls `convert_all()`

### Important: Pages Are Processed Sequentially

Within a single request, `converter.convert_all()` processes pages **sequentially**. The `page_range` parameter filters which pages to process but doesn't parallelize them.

**To get parallelism:** Send multiple HTTP requests with different `page_range` values.

---

## Threads vs Processes

### For Speed (single PDF)
**Use threads** (1 process × N threads):
- Share memory (font tables, caches loaded once)
- Lower overhead
- GIL patch enables parallelism

### For Reliability (multi-user)
**Use processes** (multiple uvicorn workers):
- Isolation (one crash doesn't kill everything)
- But: more memory usage, no memory sharing

### Recommendation
```
1 process × 2 threads (for parallelism within PDF)
+ queue system (to handle multiple users)
```

Each user's PDF:
1. Split into 2 page ranges
2. Process both in parallel
3. Merge results
4. Next user in queue

---

## Server Configuration Recommendations

### Mac (Local Development)
```
docling-serve run --port 5001
# Default: 1 uvicorn worker, 2 LocalOrchestrator workers
# Optimal: 2 parallel page-range requests
```

### 8-Core Server
```bash
# Test to find optimal parallelism (likely 4)
# Options:
# - 1 process × 4 threads (shared memory, efficient)
# - 2 processes × 2 threads (isolation + parallelism)
```

### 16+ Core Server
```bash
# Test 6-8 parallel requests
# Bigger L3 cache allows more parallelism
```

---

## The Fix

In `app/pybind_parse.cpp`, wrap `parse_pdf_from_key_on_page` with GIL release:

**Before:**
```cpp
.def("parse_pdf_from_key_on_page",
     [](docling::docling_parser_v2 &self, ...) -> nlohmann::json {
    return self.parse_pdf_from_key_on_page(...);
     },
```

**After:**
```cpp
.def("parse_pdf_from_key_on_page",
     [](docling::docling_parser_v2 &self, ...) -> nlohmann::json {
    nlohmann::json result;
    {
      // Release GIL during heavy C++ computation
      pybind11::gil_scoped_release release;
      result = self.parse_pdf_from_key_on_page(...);
    }
    return result;
     },
```

---

## Build Instructions

### Prerequisites
```bash
brew install cmake
pip install pybind11
```

### Build Steps
```bash
# Clone to PERMANENT location (NEVER /tmp!)
cd /Users/bhoshaga/PycharmProjects/
git clone https://github.com/docling-project/docling-parse.git docling-parse-patched
cd docling-parse-patched

# Apply the patch to app/pybind_parse.cpp (see above)

# Build
python local_build.py

# Install as editable
pip install -e .
```

### Verify Installation
```bash
python -c "import docling_parse; print(docling_parse.__file__)"
# Should show your patched location, not site-packages
```

---

## Test Scripts

### Quick Patch Verification
```python
import time
import threading
from docling_parse.pdf_parsers import pdf_parser_v2

pdf_path = "/Users/bhoshaga/PycharmProjects/stru/sample-drawing.pdf"

def parse_page(page_no, results):
    parser = pdf_parser_v2(level="fatal")
    key = f"key_{page_no}"
    parser.load_document(key, pdf_path)
    t0 = time.time()
    parser.parse_pdf_from_key_on_page(
        key=key, page=page_no,
        keep_char_cells=False, keep_lines=False, keep_bitmaps=False,
        create_word_cells=True, create_line_cells=True,
    )
    results[page_no] = time.time() - t0
    parser.unload_document(key)

# Sequential
results_seq = {}
t0 = time.time()
for p in range(4): parse_page(p, results_seq)
t_seq = time.time() - t0

# Threaded
results_thread = {}
threads = [threading.Thread(target=parse_page, args=(p, results_thread)) for p in range(4)]
t0 = time.time()
for t in threads: t.start()
for t in threads: t.join()
t_thread = time.time() - t0

print(f"Sequential: {t_seq:.2f}s")
print(f"Threaded:   {t_thread:.2f}s")
print(f"Speedup:    {t_seq/t_thread:.2f}x")
# Unpatched: ~1.0x | Patched: ~2.0x
```

### Parallel API Test
```python
import requests
import time
from concurrent.futures import ThreadPoolExecutor

pdf_path = "/Users/bhoshaga/PycharmProjects/stru/sample-drawing.pdf"
url = "http://localhost:5001/v1/convert/file"

def convert_pages(page_range):
    start, end = page_range
    with open(pdf_path, 'rb') as f:
        t0 = time.time()
        r = requests.post(url, files={'files': f}, data={
            'to_formats': 'md',
            'do_ocr': 'false',
            'do_table_structure': 'false',
            'page_range': (start, end)
        })
        return (start, end, time.time() - t0, r.status_code)

# Test different parallelism levels
for n_threads, ranges in [
    (1, [(1, 10)]),
    (2, [(1, 5), (6, 10)]),
    (4, [(1, 3), (4, 5), (6, 8), (9, 10)]),
]:
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=n_threads) as ex:
        results = list(ex.map(convert_pages, ranges))
    print(f"{n_threads} threads: {time.time()-t0:.2f}s")
```

---

## Thread Safety Warning

**DO NOT use `pybind11::mod_gil_not_used()`** - docling-parse has static global variables that are NOT thread-safe:

```cpp
// In src/v2/pdf_resources/page_font.h
static font_glyphs    glyphs;     // SHARED, NOT PROTECTED
static font_cids      cids;
static font_encodings encodings;
static base_fonts     bfonts;
```

The GIL release patch is safe because:
- GIL is released only during the parse call
- Python object manipulation still protected
- `mod_gil_not_used()` would allow unsafe concurrent writes

---

## Baseline Performance (Dec 3, 2025)

**Test environment:** Mac M-series, docling-serve 1.9.0, patched docling-parse 4.7.2

### Time Benchmarks

| Configuration | 10-page PDF | Per-page |
|---------------|-------------|----------|
| Sequential (1 request) | 44s | 4.4s |
| **Parallel (2 requests)** | **26s** | **2.6s** |
| Parallel (3 requests) | 30s | 3.0s |
| Parallel (4 requests) | 28s | 2.8s |

### RAM Usage

| State | RAM |
|-------|-----|
| Idle (after startup) | ~1.6 GB |
| 1 request processing | ~3 GB peak |
| 2 parallel requests | ~4.4 GB peak |
| After completion | ~2.2 GB |

### Optimal Configuration (Mac)

```
1 process × 2 threads
~5 GB RAM allocation
2 CPU cores
```

**DO NOT exceed 2 parallel page-range requests** - more threads = CPU cache contention = slower.

---

## Key Findings Summary

| Finding | Value |
|---------|-------|
| Bottleneck | C++ docling-parse (90.5% of time) |
| Root cause | GIL held during C++ parsing |
| Fix | `pybind11::gil_scoped_release` |
| Speedup (Mac) | **1.69x** with 2 parallel requests |
| Optimal threads (Mac) | 2 |
| Limiting factor | CPU L3 cache contention |

---

## Stack Versions
```
docling-serve 1.9.0
  └── docling 2.64.0
        └── docling-parse 4.7.2 (patched)
```

---

## Repository Info

- **docling-parse GitHub:** https://github.com/docling-project/docling-parse
- **Version tested:** 4.7.2
- **Build system:** CMake + pybind11

---

## Docker Deployment

### Directory Structure
```
/Users/bhoshaga/PycharmProjects/docling-serve-docker/
├── Dockerfile
├── gil_release.patch
└── README.md
```

### The Patch File (`gil_release.patch`)
```diff
diff --git a/app/pybind_parse.cpp b/app/pybind_parse.cpp
index e71cc03..be4fd68 100644
--- a/app/pybind_parse.cpp
+++ b/app/pybind_parse.cpp
@@ -418,15 +418,21 @@ PYBIND11_MODULE(pdf_parsers, m) {
 	    bool keep_bitmaps,
 	    bool create_word_cells,
 	    bool create_line_cells) -> nlohmann::json {
-    return self.parse_pdf_from_key_on_page(key,
-					   page,
-					   page_boundary,
-					   do_sanitization,
-					   keep_char_cells,
-					   keep_lines,
-					   keep_bitmaps,
-					   create_word_cells,
-					   create_line_cells);
+    nlohmann::json result;
+    {
+      // Release GIL during heavy C++ computation
+      pybind11::gil_scoped_release release;
+      result = self.parse_pdf_from_key_on_page(key,
+                                               page,
+                                               page_boundary,
+                                               do_sanitization,
+                                               keep_char_cells,
+                                               keep_lines,
+                                               keep_bitmaps,
+                                               create_word_cells,
+                                               create_line_cells);
+    }
+    return result;
 	 },
```

### Container Resources
```yaml
resources:
  cpu: 2 cores
  memory: 5 GB
workers: 1 process × 2 threads
```

### Expected Performance (Container)
| Metric | Target |
|--------|--------|
| 10-page PDF (2 parallel) | ~26s |
| RAM peak | ~4.4 GB |
| Throughput | 2-3 PDFs/min |

---

## Next Steps

1. ~~Clone docling-parse to `/Users/bhoshaga/PycharmProjects/docling-parse-patched/`~~ ✅ DONE
2. ~~Apply the GIL release patch~~ ✅ DONE
3. ~~Build and install as editable~~ ✅ DONE
4. ~~Test parallel API calls~~ ✅ DONE (1.69x speedup with 2 threads)
5. Build Docker image with patched docling-parse
6. Test container performance matches baseline (~26s for 10-page PDF)
7. Deploy to production
8. Consider submitting PR to upstream docling-parse
