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
    is_stub: bool = False

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        if hasattr(cls, "provider") and cls.provider != ATSProvider.UNKNOWN:
            _REGISTRY[cls.provider] = cls

    @abstractmethod
    def parse(self, content: str, *, url: str) -> list[JobListing]:
        """Parse raw content into structured job listings."""
        ...

    @classmethod
    def for_provider(
        cls,
        provider: ATSProvider,
        *,
        llm_provider: str | None = None,
        api_base: str | None = None,
    ) -> BaseParser:
        """Return the parser instance for a given ATS provider.

        Stub parsers (``is_stub=True``) automatically fall through to
        :class:`~strata_harvest.parsers.llm_fallback.LLMFallbackParser`.
        """
        from strata_harvest.parsers.llm_fallback import LLMFallbackParser

        parser_cls = _REGISTRY.get(provider, LLMFallbackParser)
        if parser_cls.is_stub or parser_cls is LLMFallbackParser:
            if llm_provider or api_base:
                return LLMFallbackParser(llm_provider=llm_provider, api_base=api_base)
            return LLMFallbackParser()
        return parser_cls()

    @classmethod
    def is_stub_provider(cls, provider: ATSProvider) -> bool:
        """Return True if the registered parser for *provider* is a stub."""
        parser_cls = _REGISTRY.get(provider)
        return parser_cls is not None and parser_cls.is_stub
