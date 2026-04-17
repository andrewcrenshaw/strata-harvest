"""Token-minimizing extraction pipeline for job listings.

Provides a 5-tier pipeline that maximizes extraction quality while minimizing LLM token usage:

1. Structured data extraction (JSON-LD, microdata, OpenGraph) — zero LLM tokens
2. CSS/XPath parsing for known ATS shells
3. trafilatura content extraction to Markdown
4. Local LLM (Ollama Qwen2.5-7B-Instruct) with instructor + Pydantic
5. Hosted Gemini fallback (when local LLM unavailable and opt-in enabled)
"""

from __future__ import annotations

__all__ = [
    "JobPostingSchema",
    "extract_with_pipeline",
]

from strata_harvest.extract.pipeline import extract_with_pipeline
from strata_harvest.extract.schema import JobPostingSchema
