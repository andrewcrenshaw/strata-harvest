"""Base parser interface for ATS-specific implementations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from strata_harvest.models import ATSProvider

if TYPE_CHECKING:
    from strata_harvest.models import JobListing

_REGISTRY: dict[ATSProvider, type[BaseParser]] = {}


class BaseParser(ABC):
    """Abstract base class for ATS parsers."""

    provider: ATSProvider = ATSProvider.UNKNOWN

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "provider") and cls.provider != ATSProvider.UNKNOWN:
            _REGISTRY[cls.provider] = cls

    @abstractmethod
    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse raw content into structured job listings."""
        ...

    @classmethod
    def for_provider(cls, provider: ATSProvider) -> BaseParser:
        """Return the parser instance for a given ATS provider."""
        from strata_harvest.parsers.llm_fallback import LLMFallbackParser

        parser_cls = _REGISTRY.get(provider, LLMFallbackParser)
        return parser_cls()
