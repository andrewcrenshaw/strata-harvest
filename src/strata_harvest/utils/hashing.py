"""Content hashing for change detection and deduplication."""

from __future__ import annotations

import hashlib
import re


def content_hash(text: str) -> str:
    """SHA-256 hash of normalized content for change detection.

    Normalizes whitespace before hashing so cosmetic page changes
    don't trigger false positives.
    """
    normalized = re.sub(r"\s+", " ", text.strip())
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def stable_listing_key(
    *,
    external_id: str | None = None,
    url: str | None = None,
    title: str | None = None,
    content: str | None = None,
) -> str:
    """Generate a stable key for a job listing, preferring specificity.

    Priority: external_id > url > title hash > content hash.
    """
    if external_id:
        return f"id:{external_id}"
    if url:
        return f"url:{url}"
    if title:
        return f"title:{hashlib.sha256(title.encode('utf-8')).hexdigest()[:16]}"
    if content:
        return f"content:{content_hash(content)[:16]}"
    return "unknown"
