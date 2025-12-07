#!/usr/bin/env python3
"""Memory trace test - find where the leak is"""

import gc
import os
import sys
import psutil

def get_memory_mb():
    """Get current process memory in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def log_memory(label):
    mem = get_memory_mb()
    print(f"[{mem:,.1f} MB] {label}")
    return mem

print("=" * 60)
print("MEMORY TRACE TEST - ISOLATING COMPONENTS")
print("=" * 60)

log_memory("START")

# Test 1: Just load the PDF backend (no ML models)
print("\n=== TEST: PDF BACKEND ONLY (no ML models) ===")
log_memory("Before imports")

from io import BytesIO
from pathlib import Path
from docling.backend.docling_parse_v4_backend import DoclingParseV4DocumentBackend
from docling.datamodel.document import InputDocument

log_memory("After imports")

pdf_path = Path("/Users/bhoshaga/stru-docling/sample-drawing.pdf")

from docling.datamodel.base_models import InputFormat

for run in range(1, 4):
    print(f"\n--- BACKEND ONLY RUN {run} ---")
    mem_before = log_memory(f"Before loading PDF {run}")

    # Create input document
    in_doc = InputDocument(
        path_or_stream=pdf_path,
        format=InputFormat.PDF,
        backend=DoclingParseV4DocumentBackend,
    )
    log_memory(f"After InputDocument {run}")

    # Create backend
    backend = DoclingParseV4DocumentBackend(in_doc, pdf_path)
    log_memory(f"After creating backend {run}")

    # Load pages
    page_count = backend.page_count()
    print(f"  Page count: {page_count}")

    for page_no in range(min(2, page_count)):
        page_backend = backend.load_page(page_no)
        log_memory(f"After load_page({page_no}) {run}")

        # Get page size
        size = page_backend.get_size()
        log_memory(f"After get_size() {run}")

        # Get text cells
        cells = list(page_backend.get_text_cells())
        log_memory(f"After get_text_cells() ({len(cells)} cells) {run}")

        # Render page image at scale 1.0
        img = page_backend.get_page_image(scale=1.0)
        log_memory(f"After get_page_image() ({img.size}) {run}")

        # Delete image
        del img
        gc.collect()
        log_memory(f"After del img + gc {run}")

        # Unload page
        page_backend.unload()
        log_memory(f"After page_backend.unload() {run}")

        del page_backend
        gc.collect()
        log_memory(f"After del page_backend + gc {run}")

    # Unload backend
    backend.unload()
    log_memory(f"After backend.unload() {run}")

    del backend
    del in_doc
    gc.collect()

    mem_after = log_memory(f"After full cleanup {run}")
    print(f"  Memory increase this run: {mem_after - mem_before:.1f} MB")

print("\n=== FINAL MEMORY ===")
log_memory("END")
