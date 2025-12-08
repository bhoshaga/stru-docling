"""
PDF utilities for page splitting and counting.

Uses PyMuPDF (fitz) for ~10x faster PDF operations compared to pypdf.
"""
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple

import fitz  # PyMuPDF

# Thread pool for parallel PDF operations
_pdf_pool = ThreadPoolExecutor(max_workers=8)


def get_page_count(pdf_bytes: bytes) -> int:
    """Get the number of pages in a PDF."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    count = doc.page_count
    doc.close()
    return count


def get_page_sizes(pdf_bytes: bytes) -> List[int]:
    """Get the size in bytes of each page (approximate via content streams)."""
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    sizes = []
    for i in range(doc.page_count):
        # Approximate size by extracting single page
        single = fitz.open()
        single.insert_pdf(doc, from_page=i, to_page=i)
        sizes.append(len(single.tobytes()))
        single.close()
    doc.close()
    return sizes


def extract_pages(pdf_bytes: bytes, start_page: int, end_page: int) -> bytes:
    """
    Extract a range of pages from a PDF into a new PDF.

    Args:
        pdf_bytes: Original PDF as bytes
        start_page: First page to extract (1-indexed)
        end_page: Last page to extract (1-indexed, inclusive)

    Returns:
        New PDF containing only the specified pages as bytes
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    output = fitz.open()

    # PyMuPDF uses 0-indexed pages
    output.insert_pdf(doc, from_page=start_page - 1, to_page=end_page - 1)

    result = output.tobytes()
    output.close()
    doc.close()
    return result


def split_page_ranges(total_pages: int, num_workers: int, user_range: Tuple[int, int] = None) -> List[Tuple[int, int]]:
    """
    Split pages across workers as evenly as possible.

    Args:
        total_pages: Total number of pages in PDF
        num_workers: Number of workers to split across
        user_range: Optional (start, end) range requested by user (1-indexed)

    Returns:
        List of (start_page, end_page) tuples (1-indexed, inclusive)
    """
    # Apply user range if specified
    if user_range:
        start, end = user_range
        start = max(1, min(start, total_pages))
        end = max(start, min(end, total_pages))
        pages_to_process = end - start + 1
        offset = start - 1
    else:
        pages_to_process = total_pages
        offset = 0

    # Don't use more workers than pages
    actual_workers = min(num_workers, pages_to_process)

    if actual_workers == 0:
        return []

    base = pages_to_process // actual_workers
    remainder = pages_to_process % actual_workers

    splits = []
    current = 1 + offset  # Start from user's start page (or 1)

    for i in range(actual_workers):
        # First 'remainder' workers get one extra page
        count = base + (1 if i < remainder else 0)
        if count > 0:
            end = current + count - 1
            splits.append((current, end))
            current = end + 1

    return splits


def split_pdf_for_workers(
    pdf_bytes: bytes,
    num_workers: int,
    user_range: Tuple[int, int] = None
) -> List[Tuple[bytes, Tuple[int, int]]]:
    """
    Split a PDF into chunks for parallel processing.

    Args:
        pdf_bytes: Original PDF as bytes
        num_workers: Number of workers to split across
        user_range: Optional (start, end) range requested by user

    Returns:
        List of (chunk_bytes, original_page_range) tuples
    """
    total_pages = get_page_count(pdf_bytes)
    page_ranges = split_page_ranges(total_pages, num_workers, user_range)

    # Extract all chunks in parallel using thread pool
    def extract_chunk(page_range):
        start, end = page_range
        chunk_bytes = extract_pages(pdf_bytes, start, end)
        return (chunk_bytes, (start, end))

    # Submit all extractions to thread pool
    futures = [_pdf_pool.submit(extract_chunk, pr) for pr in page_ranges]
    chunks = [f.result() for f in futures]

    return chunks
