"""Local LLM extraction via Ollama + instructor.

Uses instructor library for Pydantic-guided structured extraction from Ollama.
Supports Qwen2.5-7B-Instruct with xgrammar-backed JSON constrained decoding.

Requires: pip install strata-harvest[extract,local-llm]
"""

from __future__ import annotations

import logging
from typing import TypeVar

logger = logging.getLogger(__name__)

try:
    from instructor import from_ollama  # type: ignore[attr-defined]

    _INSTRUCTOR_AVAILABLE = True
except ImportError:
    _INSTRUCTOR_AVAILABLE = False

try:
    import ollama  # noqa: F401 — Used for availability check

    _OLLAMA_AVAILABLE = True
except ImportError:
    _OLLAMA_AVAILABLE = False

T = TypeVar("T")


class OllamaExtractor:
    """Extract structured data from text using local Ollama model.

    Wraps instructor + Ollama for Pydantic-guided extraction.
    Requires Ollama running at http://localhost:11434 (configurable).

    Parameters
    ----------
    model:
        Ollama model name (default: "qwen2.5:7b-instruct-q4_K_M").
        Must support instruction following and JSON output.
    base_url:
        Ollama API base URL (default: "http://localhost:11434").
    timeout:
        Request timeout in seconds (default: 30).

    Examples
    --------
    >>> extractor = OllamaExtractor()
    >>> from strata_harvest.extract.schema import JobPostingSchema
    >>> jobs = extractor.extract(html_text, JobPostingSchema)
    >>> for job in jobs:
    ...     print(job.title)
    """

    def __init__(
        self,
        *,
        model: str = "qwen2.5:7b-instruct-q4_K_M",
        base_url: str = "http://localhost:11434",
        timeout: int = 30,
    ) -> None:
        self.model = model
        self.base_url = base_url
        self.timeout = timeout
        self._client: object | None = None

    def is_available(self) -> bool:
        """Check if Ollama is reachable and model is loaded."""
        if not _OLLAMA_AVAILABLE:
            return False
        try:
            # Try to list models from Ollama instance
            from ollama import Client

            client = Client(host=self.base_url)
            response = client.list()
            models = [m.get("name", "") for m in response.get("models", [])]
            return any(self.model in m for m in models)
        except Exception as exc:
            logger.debug("Ollama health check failed: %s", exc)
            return False

    def extract(
        self,
        text: str,
        schema: type[T],
        *,
        instruction: str = "Extract structured data from the provided text.",
    ) -> T | None:
        """Extract a single structured object from text.

        Parameters
        ----------
        text:
            Input text to extract from.
        schema:
            Pydantic model class for the extraction.
        instruction:
            Extraction instruction to send to the model.

        Returns
        -------
        Optional[T]
            Extracted object, or None on failure.
        """
        if not _INSTRUCTOR_AVAILABLE or not _OLLAMA_AVAILABLE:
            logger.warning(
                "instructor and ollama required: pip install strata-harvest[extract,local-llm]"
            )
            return None

        if not text or not text.strip():
            return None

        try:
            client = from_ollama(model=self.model, base_url=self.base_url)
            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": instruction},
                    {"role": "user", "content": text},
                ],
                response_model=schema,
                temperature=0.1,
            )
            return response  # type: ignore[no-any-return]
        except Exception as exc:
            logger.debug("Ollama extraction failed: %s", exc)
            return None

    def extract_list(
        self,
        text: str,
        schema: type[T],
        *,
        instruction: str = "Extract all structured data items from the text.",
    ) -> list[T]:
        """Extract a list of structured objects from text.

        Parameters
        ----------
        text:
            Input text to extract from.
        schema:
            Pydantic model class for each item.
        instruction:
            Extraction instruction to send to the model.

        Returns
        -------
        list[T]
            List of extracted objects (empty on failure).
        """
        if not _INSTRUCTOR_AVAILABLE or not _OLLAMA_AVAILABLE:
            logger.warning(
                "instructor and ollama required: pip install strata-harvest[extract,local-llm]"
            )
            return []

        if not text or not text.strip():
            return []

        try:
            client = from_ollama(model=self.model, base_url=self.base_url)
            # Wrap in container for list extraction
            from pydantic import BaseModel

            class Container(BaseModel):
                items: list[schema]  # type: ignore

            response = client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": instruction},
                    {"role": "user", "content": text},
                ],
                response_model=Container,
                temperature=0.1,
            )
            return response.items if response else []
        except Exception as exc:
            logger.debug("Ollama list extraction failed: %s", exc)
            return []
