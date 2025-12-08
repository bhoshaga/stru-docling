"""
Result merging utilities for combining multi-worker document results.

These are pure functions with no state dependencies.
"""

from typing import List, Tuple


def merge_document_results(sub_results: List[dict], filename: str) -> dict:
    """
    Merge document results from multiple workers into a single response.

    Args:
        sub_results: List of (page_range, result) from each worker
        filename: Original filename

    Returns:
        Merged ConvertDocumentResponse matching docling-serve format
    """
    if not sub_results:
        return {
            "document": {"filename": filename},
            "status": "failure",
            "errors": [{"message": "No results to merge"}],
            "processing_time": 0,
            "timings": {},
        }

    # Sort sub_results by page range start
    sorted_results = sorted(sub_results, key=lambda x: x["pages"][0])

    # Initialize merged document
    merged_doc = {
        "filename": filename,
        "md_content": None,
        "json_content": None,
        "html_content": None,
        "text_content": None,
        "doctags_content": None,
    }

    total_time = 0
    all_errors = []
    all_timings = {"per_page": []}
    has_success = False

    # Track content to merge
    md_parts = []
    html_parts = []
    text_parts = []
    doctags_parts = []
    json_contents = []

    for item in sorted_results:
        result = item.get("result", {})
        doc = result.get("document", {})
        pages = item.get("pages", (0, 0))

        # Track status
        if result.get("status") == "success":
            has_success = True

        # Aggregate times and errors
        page_processing_time = result.get("processing_time", 0)
        total_time += page_processing_time
        all_errors.extend(result.get("errors", []))

        # Collect per-page timings (keep ALL page timings)
        page_timings = result.get("timings", {})
        all_timings["per_page"].append({
            "pages": list(pages),
            "processing_time": page_processing_time,
            **page_timings
        })

        # Collect string content
        if doc.get("md_content"):
            md_parts.append(doc["md_content"])
        if doc.get("html_content"):
            html_parts.append(doc["html_content"])
        if doc.get("text_content"):
            text_parts.append(doc["text_content"])
        if doc.get("doctags_content"):
            doctags_parts.append(doc["doctags_content"])
        if doc.get("json_content"):
            json_contents.append((item["pages"], doc["json_content"]))

    # Merge string content (simple concatenation)
    if md_parts:
        merged_doc["md_content"] = "\n\n".join(md_parts)
    if html_parts:
        merged_doc["html_content"] = "\n".join(html_parts)
    if text_parts:
        merged_doc["text_content"] = "\n\n".join(text_parts)
    if doctags_parts:
        merged_doc["doctags_content"] = "\n".join(doctags_parts)

    # Merge JSON content (more complex)
    if json_contents:
        merged_doc["json_content"] = _merge_json_content(json_contents, filename)

    return {
        "document": merged_doc,
        "status": "success" if has_success else "failure",
        "errors": all_errors,
        "processing_time": total_time,
        "timings": all_timings,
    }


def _merge_json_content(json_contents: List[Tuple[tuple, dict]], filename: str) -> dict:
    """
    Merge json_content from multiple workers.

    Args:
        json_contents: List of ((start_page, end_page), json_content) tuples

    Note: docling-serve returns page numbers relative to each chunk (1, 2, 3...)
    not the original PDF page numbers. We need to renumber them.
    """
    if not json_contents:
        return None

    # Use first chunk as base
    first_range, first_jc = json_contents[0]
    base = first_jc.copy()

    # Lists to concatenate
    list_keys = ["groups", "texts", "pictures", "tables", "key_value_items", "form_items"]

    # Renumber pages from first chunk to original page numbers
    merged_pages = {}
    for rel_page_str, page_data in first_jc.get("pages", {}).items():
        rel_page = int(rel_page_str)
        # Convert relative page (1, 2, ...) to original page number
        original_page = first_range[0] + rel_page - 1
        merged_pages[str(original_page)] = page_data

    # Merge lists and pages from subsequent chunks
    for page_range, jc in json_contents[1:]:
        # Merge list contents
        for key in list_keys:
            if key in base and key in jc:
                base[key] = base.get(key, []) + jc.get(key, [])

        # Merge pages with renumbering
        for rel_page_str, page_data in jc.get("pages", {}).items():
            rel_page = int(rel_page_str)
            # Convert relative page to original page number
            original_page = page_range[0] + rel_page - 1
            merged_pages[str(original_page)] = page_data

    base["pages"] = merged_pages
    return base
