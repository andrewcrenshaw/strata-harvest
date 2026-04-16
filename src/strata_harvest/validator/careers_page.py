"""CareersPageValidator: pre-harvest signal-hierarchy validator.

Runs before any parsing and returns a :class:`ValidationResult` indicating
whether a fetched page is a genuine career/jobs page.

Directly fixes the wrong-page false-positive class: blog posts on /careers
paths, investor-relations pages with "join us" CTAs, archived subdomains, and
wrong ATS slugs returning 404 or empty pages.

Signal hierarchy (short-circuits on first strong signal):

1. schema.org/JobPosting JSON-LD via extruct → strong positive (0.95)
2. Hard rejects: BlogPosting/NewsArticle/Article schema; og:type=article +
   article:published_time; noindex meta; canonical → /blog|/news|/press|
   /investors|/ir|/archive
3. ATS detection hit (pre-supplied ATSInfo) → strong positive (0.90)
4. URL pattern (/careers, /jobs, /positions, etc.) → weak positive
5. Title/H1 regex (careers, open roles, we are hiring, join us) → weak positive
6. Empty-result detection: stripped text < 200 chars or zero job-list DOM
   signals → mark suspect, caller should trigger Exa heal (PCC-1807)

Every rejection emits a structured log record with ``reason_code`` and
``signals`` so scrape-audit pipelines can aggregate ``wrong_page_rate``.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

from strata_harvest.models import ATSInfo, ATSProvider

logger = logging.getLogger(__name__)

try:
    import extruct as _extruct

    _EXTRUCT_AVAILABLE = True
except ImportError:
    _EXTRUCT_AVAILABLE = False

# ---------------------------------------------------------------------------
# Reason codes — keep stable; used as metric labels for wrong_page_rate
# ---------------------------------------------------------------------------
RC_SCHEMA_JOB_POSTING = "schema_job_posting"
RC_SCHEMA_NON_JOB = "schema_non_job"
RC_OG_ARTICLE = "og_article"
RC_NOINDEX = "noindex_meta"
RC_CANONICAL_NON_JOB = "canonical_non_job"
RC_ATS_DETECTED = "ats_detected"
RC_URL_PATTERN = "url_pattern"
RC_TITLE_MATCH = "title_match"
RC_EMPTY_PAGE = "empty_page"
RC_NO_SIGNALS = "no_signals"

# ---------------------------------------------------------------------------
# Schema types
# ---------------------------------------------------------------------------
_JOB_SCHEMA_TYPES: frozenset[str] = frozenset({"JobPosting"})
_NON_JOB_SCHEMA_TYPES: frozenset[str] = frozenset(
    {"BlogPosting", "NewsArticle", "Article"}
)

# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------
_CANONICAL_REJECT_RE = re.compile(
    r"/(blog|news|press|investors?|ir|archive)([/\-_?#]|$)",
    re.I,
)
_URL_JOBS_RE = re.compile(
    r"/(careers?|jobs?|positions?|openings?|join|opportunities?)([/\-_?#]|$)",
    re.I,
)
_TITLE_CAREERS_RE = re.compile(
    r"\b(careers?|open\s+roles?|we\s+are\s+hiring|join\s+us|work\s+with\s+us"
    r"|job\s+openings?)\b",
    re.I,
)
_JOB_LIST_SIGNAL_RE = re.compile(
    r'(job[-_]?card|job[-_]?listing|position[-_]?item|role[-_]?item'
    r'|opening[-_]?row|data-job-id|class=["\']job|class=["\']position'
    r'|class=["\']role|aria-label=["\']job\s+posting)',
    re.I,
)
_JSON_LD_SCRIPT_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.I,
)
_OG_TYPE_RE = re.compile(
    r'<meta[^>]+(?:'
    r'property=["\']og:type["\'][^>]+content=["\']([^"\']+)["\']'
    r'|content=["\']([^"\']+)["\'][^>]+property=["\']og:type["\']'
    r')',
    re.I,
)
_ARTICLE_PUB_TIME_RE = re.compile(r"article:published_time", re.I)
_NOINDEX_META_RE = re.compile(
    r'<meta[^>]+name=["\']robots["\'][^>]+content=[^>]*noindex',
    re.I,
)
_CANONICAL_HREF_RE = re.compile(
    r'<link[^>]+(?:'
    r'rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']'
    r'|href=["\']([^"\']+)["\'][^>]+rel=["\']canonical["\']'
    r')',
    re.I,
)
_STRIP_TAGS_RE = re.compile(r"<[^>]+>")


@dataclass
class ValidationResult:
    """Result of a :class:`CareersPageValidator` run.

    Attributes
    ----------
    is_valid:
        ``True`` when the page looks like a genuine career/jobs page.
    confidence:
        Score in ``[0.0, 1.0]`` — strength of the strongest matched signal.
    reject_reason:
        Human-readable explanation when ``is_valid`` is ``False``.
    reason_code:
        Stable short label for metric aggregation (``wrong_page_rate``).
    signals:
        Ordered list of signals that fired, for debugging.
    suspect:
        ``True`` when ``is_valid`` is uncertain (sparse page, JS-hydrated).
        Caller should schedule an Exa heal (PCC-1807).
    """

    is_valid: bool
    confidence: float
    reject_reason: str | None = None
    reason_code: str | None = None
    signals: list[str] = field(default_factory=list)
    suspect: bool = False


class CareersPageValidator:
    """Validates whether a fetched page is a genuine career/jobs page.

    Instantiate once per :class:`~strata_harvest.crawler.Crawler`; the object
    is stateless and safe to call concurrently.

    Examples
    --------
    >>> validator = CareersPageValidator()
    >>> result = validator.validate("https://example.com/careers", "<html>...</html>")
    >>> result.is_valid
    True
    """

    def validate(
        self,
        url: str,
        html: str,
        *,
        ats_info: ATSInfo | None = None,
    ) -> ValidationResult:
        """Run the signal hierarchy and return a :class:`ValidationResult`.

        Parameters
        ----------
        url:
            The page URL (used for URL pattern and canonical checks).
        html:
            Fetched page HTML. May be empty when the fetch failed.
        ats_info:
            Pre-detected ATS info from ``detect_ats()``. When the provider is
            not ``UNKNOWN``, acts as a strong positive (signal #3).

        Returns
        -------
        ValidationResult
            ``is_valid``, ``confidence``, ``reject_reason``, ``reason_code``,
            ``signals``, and ``suspect`` flag.
        """
        signals: list[str] = []

        # ------------------------------------------------------------------
        # Signal 1 (strong positive) + Signal 2a (hard reject):
        # schema.org JSON-LD — JobPosting → valid, BlogPosting etc. → reject
        # ------------------------------------------------------------------
        schema_result = self._check_json_ld(html, url, signals)
        if schema_result is not None:
            return schema_result

        # ------------------------------------------------------------------
        # Signal 2b: og:type=article + article:published_time → hard reject
        # ------------------------------------------------------------------
        og_result = self._check_opengraph(html, url, signals)
        if og_result is not None:
            return og_result

        # ------------------------------------------------------------------
        # Signal 2c: <meta name="robots" content="noindex"> → hard reject
        # ------------------------------------------------------------------
        if _NOINDEX_META_RE.search(html):
            signals.append("noindex_meta")
            result = ValidationResult(
                is_valid=False,
                confidence=0.9,
                reject_reason=(
                    "Page has noindex robots meta tag — likely not a public job board"
                ),
                reason_code=RC_NOINDEX,
                signals=signals,
            )
            self._audit_reject(url, result)
            return result

        # ------------------------------------------------------------------
        # Signal 2d: canonical URL → /blog|/news|/press|/investors|/ir|/archive
        # ------------------------------------------------------------------
        canonical_result = self._check_canonical(html, url, signals)
        if canonical_result is not None:
            return canonical_result

        # ------------------------------------------------------------------
        # Signal 3 (strong positive): ATS detection hit
        # ------------------------------------------------------------------
        if ats_info is not None and ats_info.provider != ATSProvider.UNKNOWN:
            signals.append(f"ats_detected:{ats_info.provider.value}")
            return ValidationResult(
                is_valid=True,
                confidence=0.9,
                reason_code=RC_ATS_DETECTED,
                signals=signals,
            )

        # ------------------------------------------------------------------
        # Signals 4 & 5: weak positives (URL pattern, title/H1 regex)
        # ------------------------------------------------------------------
        url_positive = bool(_URL_JOBS_RE.search(url))
        if url_positive:
            signals.append("url_pattern")

        # Scan only the first 4 KB — title and H1 are always near the top
        title_positive = bool(_TITLE_CAREERS_RE.search(html[:4096]))
        if title_positive:
            signals.append("title_match")

        # ------------------------------------------------------------------
        # Signal 6: sparse/empty page detection (< 200 stripped chars)
        # Likely a JS-hydrated shell, 404, or wrong page entirely.
        # Note: "zero job-list DOM signals" is recorded in signals but does
        # NOT short-circuit here — it informs the no_signals path below.
        # ------------------------------------------------------------------
        stripped_text = _STRIP_TAGS_RE.sub(" ", html).strip()
        has_job_list_signals = bool(_JOB_LIST_SIGNAL_RE.search(html))

        if len(stripped_text) < 200:
            signals.append("empty_or_sparse")
            if not has_job_list_signals:
                signals.append("no_job_list_signals")
            if url_positive or title_positive:
                # URL/title hint suggests a jobs page but content is sparse —
                # likely JS-hydrated (e.g. Rippling). Mark suspect; trigger heal.
                return ValidationResult(
                    is_valid=True,
                    confidence=0.4,
                    reason_code=RC_EMPTY_PAGE,
                    signals=signals,
                    suspect=True,
                )
            else:
                # No hints at all and sparse content → wrong page
                result = ValidationResult(
                    is_valid=False,
                    confidence=0.75,
                    reject_reason=(
                        "Page text < 200 chars and no career page signals — "
                        "empty or wrong page"
                    ),
                    reason_code=RC_EMPTY_PAGE,
                    signals=signals,
                    suspect=True,
                )
                self._audit_reject(url, result)
                return result

        # Record missing job-list DOM signals as a soft signal (informational)
        if not has_job_list_signals:
            signals.append("no_job_list_signals")

        # ------------------------------------------------------------------
        # Combine weak positives
        # ------------------------------------------------------------------
        if url_positive and title_positive:
            return ValidationResult(
                is_valid=True,
                confidence=0.65,
                reason_code=f"{RC_URL_PATTERN}+{RC_TITLE_MATCH}",
                signals=signals,
            )
        if url_positive:
            return ValidationResult(
                is_valid=True,
                confidence=0.5,
                reason_code=RC_URL_PATTERN,
                signals=signals,
            )
        if title_positive:
            return ValidationResult(
                is_valid=True,
                confidence=0.4,
                reason_code=RC_TITLE_MATCH,
                signals=signals,
            )

        # No positive signals at all
        result = ValidationResult(
            is_valid=False,
            confidence=0.55,
            reject_reason="No career page signals found",
            reason_code=RC_NO_SIGNALS,
            signals=signals,
        )
        self._audit_reject(url, result)
        return result

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _check_json_ld(
        self,
        html: str,
        url: str,
        signals: list[str],
    ) -> ValidationResult | None:
        """Parse JSON-LD blocks; return result on strong signal or None."""
        schemas = self._extract_json_ld(html)

        # Collect all @type values across all schema blocks (flattening @graph)
        all_types: list[str] = []
        for schema in schemas:
            all_types.extend(_collect_types(schema))

        if any(t in _JOB_SCHEMA_TYPES for t in all_types):
            signals.append("schema_job_posting")
            return ValidationResult(
                is_valid=True,
                confidence=0.95,
                reason_code=RC_SCHEMA_JOB_POSTING,
                signals=signals,
            )

        # Check non-job types only when no JobPosting was found
        reject_types = [t for t in all_types if t in _NON_JOB_SCHEMA_TYPES]
        if reject_types:
            label = reject_types[0]
            signals.append(f"schema_non_job:{label}")
            result = ValidationResult(
                is_valid=False,
                confidence=0.9,
                reject_reason=f"Schema.org type {label!r} indicates non-job content",
                reason_code=RC_SCHEMA_NON_JOB,
                signals=signals,
            )
            self._audit_reject(url, result)
            return result

        return None

    def _extract_json_ld(self, html: str) -> list[dict]:  # type: ignore[type-arg]
        """Extract JSON-LD objects from HTML.

        Uses extruct when available for robust parsing; falls back to regex +
        ``json.loads`` so tests and offline environments work without the dep.
        """
        if _EXTRUCT_AVAILABLE:
            try:
                data = _extruct.extract(html, syntaxes=["json-ld"], uniform=True)
                items = data.get("json-ld", [])
                return [i for i in items if isinstance(i, dict)]
            except Exception:
                pass  # fall through to regex

        results: list[dict] = []  # type: ignore[type-arg]
        for match in _JSON_LD_SCRIPT_RE.finditer(html):
            try:
                obj = json.loads(match.group(1))
                if isinstance(obj, list):
                    results.extend(o for o in obj if isinstance(o, dict))
                elif isinstance(obj, dict):
                    results.append(obj)
            except json.JSONDecodeError:
                pass
        return results

    def _check_opengraph(
        self,
        html: str,
        url: str,
        signals: list[str],
    ) -> ValidationResult | None:
        """Reject when og:type=article AND article:published_time present."""
        og_match = _OG_TYPE_RE.search(html)
        if og_match:
            og_type = (og_match.group(1) or og_match.group(2) or "").lower()
            if og_type == "article" and _ARTICLE_PUB_TIME_RE.search(html):
                signals.append("og_article+published_time")
                result = ValidationResult(
                    is_valid=False,
                    confidence=0.85,
                    reject_reason=(
                        "OpenGraph type is 'article' with article:published_time"
                        " — this is a news or blog page"
                    ),
                    reason_code=RC_OG_ARTICLE,
                    signals=signals,
                )
                self._audit_reject(url, result)
                return result
        return None

    def _check_canonical(
        self,
        html: str,
        url: str,
        signals: list[str],
    ) -> ValidationResult | None:
        """Reject when canonical URL path points to a non-job section."""
        canonical_match = _CANONICAL_HREF_RE.search(html)
        if canonical_match:
            canonical_url = canonical_match.group(1) or canonical_match.group(2) or ""
            if _CANONICAL_REJECT_RE.search(canonical_url):
                signals.append(f"canonical_non_job:{canonical_url}")
                result = ValidationResult(
                    is_valid=False,
                    confidence=0.9,
                    reject_reason=(
                        f"Canonical URL points to non-job section: {canonical_url!r}"
                    ),
                    reason_code=RC_CANONICAL_NON_JOB,
                    signals=signals,
                )
                self._audit_reject(url, result)
                return result
        return None

    def _audit_reject(self, url: str, result: ValidationResult) -> None:
        """Emit a structured audit log record for every rejected page."""
        logger.info(
            "CareersPageValidator rejected %s [%s]",
            url,
            result.reason_code,
            extra={
                "event": "careers_page_rejected",
                "url": url,
                "reason_code": result.reason_code,
                "reject_reason": result.reject_reason,
                "confidence": result.confidence,
                "signals": result.signals,
            },
        )


def _collect_types(schema: dict) -> list[str]:  # type: ignore[type-arg]
    """Recursively collect @type values from a schema object and its @graph."""
    types: list[str] = []
    raw = schema.get("@type", [])
    if isinstance(raw, str):
        types.append(raw)
    elif isinstance(raw, list):
        types.extend(t for t in raw if isinstance(t, str))

    for item in schema.get("@graph", []):
        if isinstance(item, dict):
            types.extend(_collect_types(item))

    return types
