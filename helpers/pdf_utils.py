"""
PDF utilities for page splitting and counting.
"""
from io import BytesIO
from typing import List, Tuple

from pypdf import PdfReader, PdfWriter


def get_page_count(pdf_bytes: bytes) -> int:
    """Get the number of pages in a PDF."""
    reader = PdfReader(BytesIO(pdf_bytes))
    return len(reader.pages)


def get_page_sizes(pdf_bytes: bytes) -> List[int]:
    """Get the size in bytes of each page (approximate via content streams)."""
    reader = PdfReader(BytesIO(pdf_bytes))
    sizes = []
    for page in reader.pages:
        # Approximate size by serializing page to bytes
        writer = PdfWriter()
        writer.add_page(page)
        buf = BytesIO()
        writer.write(buf)
        sizes.append(buf.tell())
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
    reader = PdfReader(BytesIO(pdf_bytes))
    writer = PdfWriter()

    # Convert to 0-indexed
    for i in range(start_page - 1, end_page):
        if i < len(reader.pages):
            writer.add_page(reader.pages[i])

    output = BytesIO()
    writer.write(output)
    return output.getvalue()


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

    chunks = []
    for start, end in page_ranges:
        chunk_bytes = extract_pages(pdf_bytes, start, end)
        chunks.append((chunk_bytes, (start, end)))

    return chunks
