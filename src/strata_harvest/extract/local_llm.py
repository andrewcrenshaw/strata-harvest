"""Local LLM extraction: oMLX OpenAI-compat API (primary) + Ollama fallback (deprecated).

OmlxExtractor: uses LiteLLM to call oMLX's /v1/chat/completions endpoint.
OllamaExtractor: legacy Ollama+instructor path — deprecated, kept for one release cycle.

Requires: pip install strata-harvest[llm]  (OmlxExtractor)
          pip install strata-harvest[local-llm]  (OllamaExtractor, deprecated)
"""

from __future__ import annotations

import json
import logging
import os
from typing import TypeVar

logger = logging.getLogger(__name__)

try:
    import litellm as _litellm_mod

    _LITELLM_AVAILABLE = True
except ImportError:
    _litellm_mod = None
    _LITELLM_AVAILABLE = False

try:
    from instructor import from_ollama  # type: ignore[attr-defined,unused-ignore]

    _INSTRUCTOR_AVAILABLE = True
except ImportError:
    _INSTRUCTOR_AVAILABLE = False

try:
    import ollama  # noqa: F401 — Used for availability check

    _OLLAMA_AVAILABLE = True
except ImportError:
    _OLLAMA_AVAILABLE = False

T = TypeVar("T")


# ---------------------------------------------------------------------------
# JSON helpers shared by OmlxExtractor
# ---------------------------------------------------------------------------


def _strip_fences(raw: str) -> str:
    """Remove markdown code fences from LLM response if present."""
    raw = raw.strip()
    if raw.startswith("```"):
        first_newline = raw.index("\n") if "\n" in raw else len(raw)
        raw = raw[first_newline + 1 :]
        if "```" in raw:
            raw = raw[: raw.rindex("```")]
    return raw.strip()


def _parse_single(raw: str, schema: type[T]) -> T | None:
    """Parse JSON string and validate as a single schema instance."""
    try:
        data = json.loads(_strip_fences(raw))
        return schema.model_validate(data)  # type: ignore[attr-defined,no-any-return]
    except Exception as exc:
        logger.debug("Schema parse failed: %s", exc)
        return None


def _parse_many(raw: str, schema: type[T]) -> list[T]:
    """Parse JSON string and validate as a list of schema instances.

    Accepts both ``{"items": [...]}`` envelope and bare arrays.
    """
    try:
        data = json.loads(_strip_fences(raw))
        items = data.get("items", data) if isinstance(data, dict) else data
        if not isinstance(items, list):
            return []
        return [schema.model_validate(item) for item in items]  # type: ignore[attr-defined]
    except Exception as exc:
        logger.debug("Schema list parse failed: %s", exc)
        return []


def _schema_fields_description(schema: type) -> str:
    """Return a brief field list for schema prompt construction."""
    try:
        lines = []
        for name, field in schema.model_fields.items():  # type: ignore[attr-defined]
            desc = (field.description or "").split(".")[0]
            lines.append(f"  {name}: {desc}" if desc else f"  {name}")
        return "\n".join(lines)
    except Exception:
        return schema.__name__


class OmlxExtractor:
    """Extract structured data from text using oMLX OpenAI-compatible API.

    Uses LiteLLM to call oMLX's /v1/chat/completions endpoint.
    Drop-in replacement for the deprecated OllamaExtractor.

    Requires: pip install strata-harvest[llm]

    Parameters
    ----------
    model:
        oMLX model name (default: OMLX_EXTRACT_MODEL env var or "Qwen3.6-2B-MLX-4bit").
    base_url:
        oMLX base URL (default: OMLX_BASE_URL env var or "http://studio1:8000").
    api_key:
        oMLX API key (default: OMLX_API_KEY env var or "strata1").
    timeout:
        Request timeout in seconds (default: 60).
    """

    DEFAULT_MODEL = "Qwen3.6-2B-MLX-4bit"
    DEFAULT_BASE_URL = "http://studio1:8000"
    DEFAULT_API_KEY = "strata1"

    def __init__(
        self,
        *,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
        timeout: int = 60,
    ) -> None:
        self.model = model or os.getenv("OMLX_EXTRACT_MODEL", self.DEFAULT_MODEL)
        self.base_url = base_url or os.getenv("OMLX_BASE_URL", self.DEFAULT_BASE_URL)
        self.api_key = api_key or os.getenv("OMLX_API_KEY", self.DEFAULT_API_KEY)
        self.timeout = timeout

    def is_available(self) -> bool:
        """Check if oMLX endpoint is reachable."""
        if _litellm_mod is None:
            return False
        try:
            import httpx

            resp = httpx.get(
                f"{self.base_url}/v1/models",
                timeout=5.0,
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
            return resp.status_code == 200
        except Exception as exc:
            logger.debug("oMLX health check failed: %s", exc)
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
        if _litellm_mod is None:
            logger.warning("litellm required: pip install strata-harvest[llm]")
            return None

        if not text or not text.strip():
            return None

        fields = _schema_fields_description(schema)
        system_prompt = f"{instruction}\n\nReturn a JSON object with these fields:\n{fields}"

        try:
            response = _litellm_mod.completion(
                model=f"openai/{self.model}",
                api_base=self.base_url,
                api_key=self.api_key,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
                temperature=0.1,
                timeout=self.timeout,
            )
            raw = response.choices[0].message.content if response.choices else None
            if not raw:
                return None
            return _parse_single(raw, schema)
        except Exception as exc:
            logger.debug("oMLX extraction failed: %s", exc)
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
        if _litellm_mod is None:
            logger.warning("litellm required: pip install strata-harvest[llm]")
            return []

        if not text or not text.strip():
            return []

        fields = _schema_fields_description(schema)
        system_prompt = (
            f"{instruction}\n\n"
            f"Return a JSON object with key 'items' containing an array of objects, "
            f"each with these fields:\n{fields}"
        )

        try:
            response = _litellm_mod.completion(
                model=f"openai/{self.model}",
                api_base=self.base_url,
                api_key=self.api_key,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
                temperature=0.1,
                timeout=self.timeout,
            )
            raw = response.choices[0].message.content if response.choices else None
            if not raw:
                return []
            return _parse_many(raw, schema)
        except Exception as exc:
            logger.debug("oMLX list extraction failed: %s", exc)
            return []


class OllamaExtractor:
    """Extract structured data from text using local Ollama model.

    Deprecated: use OmlxExtractor instead (PCC-2413).

    Wraps instructor + Ollama for Pydantic-guided extraction.
    Requires Ollama running at http://localhost:11434 (configurable).

    Parameters
    ----------
    model:
        Ollama model name. OllamaExtractor deprecated — use OmlxExtractor.
    base_url:
        Ollama API base URL (default: "http://localhost:11434").
    timeout:
        Request timeout in seconds (default: 30).
    """

    _DEPRECATED_MODEL = "qwen2.5:7b-instruct-q4_K_M"  # OllamaExtractor deprecated

    def __init__(
        self,
        *,
        model: str | None = None,
        base_url: str = "http://localhost:11434",
        timeout: int = 30,
    ) -> None:
        self.model = model or self._DEPRECATED_MODEL
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
