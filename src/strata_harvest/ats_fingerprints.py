"""Typed ATSFingerprint catalog for deterministic ATS provider detection.

Each entry describes how to identify a specific ATS from:
  - url_patterns: regex strings matched against the full page URL
  - dom_selectors: regex strings matched against raw HTML
  - js_globals: JavaScript global variable names expected in the page source
  - api_hints: URL templates for the provider's JSON API ({slug} is substituted)

Detection order: URL pattern → DOM selector → JS global → LLM fallback.

Wappalyzer-style coverage extended with ATS-specific signals.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from strata_harvest.models import ATSProvider


@dataclass(frozen=True)
class ATSFingerprint:
    """Fingerprint for deterministic ATS provider identification.

    Attributes
    ----------
    provider:
        The ATS provider this fingerprint identifies.
    url_patterns:
        Regex patterns matched against the full page URL (case-insensitive).
        Match → high-confidence (0.9) identification without fetching the page.
    dom_selectors:
        Regex patterns matched against raw page HTML (case-insensitive).
        Used when URL patterns are inconclusive (white-label domains).
    js_globals:
        JavaScript global variable name fragments found in page source.
        Supplement dom_selectors for SPAs where class names are minified.
    api_hints:
        Stable JSON API URL templates.  ``{slug}`` is replaced with the
        company slug extracted from the URL or DOM.
    confidence:
        Base confidence when a URL pattern matches (DOM matches use 0.85).
    """

    provider: ATSProvider
    url_patterns: list[str] = field(default_factory=list)
    dom_selectors: list[str] = field(default_factory=list)
    js_globals: list[str] = field(default_factory=list)
    api_hints: list[str] = field(default_factory=list)
    confidence: float = 0.9


# ---------------------------------------------------------------------------
# Catalog — ordered from most-specific to most-generic
# ---------------------------------------------------------------------------

FINGERPRINTS: list[ATSFingerprint] = [
    # ------------------------------------------------------------------
    # Teamtailor — {slug}.teamtailor.com  /  white-label common
    # API: api.teamtailor.com (JSON:API)
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.TEAMTAILOR,
        url_patterns=[
            r"\.teamtailor\.com",
            r"api\.teamtailor\.com",
        ],
        dom_selectors=[
            r"teamtailor",
            r"data-teamtailor",
            r"tt-job",
            r"teamtailor-job",
        ],
        js_globals=["Teamtailor", "teamtailor"],
        api_hints=[
            "https://api.teamtailor.com/v1/jobs",
        ],
    ),
    # ------------------------------------------------------------------
    # Recruitee — {slug}.recruitee.com  /  API: /api/offers
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.RECRUITEE,
        url_patterns=[
            r"\.recruitee\.com",
        ],
        dom_selectors=[
            r"recruitee",
            r"rt-job",
            r"recruitee-job",
            r"class=['\"][^'\"]*recruitee",
        ],
        js_globals=["Recruitee", "recruitee"],
        api_hints=[
            "https://{slug}.recruitee.com/api/offers",
        ],
    ),
    # ------------------------------------------------------------------
    # Pinpoint — {slug}.pinpointhq.com  /  public JSON feed
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.PINPOINT,
        url_patterns=[
            r"\.pinpointhq\.com",
        ],
        dom_selectors=[
            r"pinpointhq",
            r"pinpoint-job",
            r"data-pinpoint",
        ],
        js_globals=["Pinpoint", "PinpointHQ"],
        api_hints=[
            "https://{slug}.pinpointhq.com/jobs.json",
        ],
    ),
    # ------------------------------------------------------------------
    # Breezy HR — {slug}.breezy.hr  /  JSON at /json
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.BREEZY,
        url_patterns=[
            r"\.breezy\.hr",
        ],
        dom_selectors=[
            r"breezy\.hr",
            r"breezy-position",
            r"data-breezy",
        ],
        js_globals=["breezy", "Breezy"],
        api_hints=[
            "https://{slug}.breezy.hr/json",
        ],
    ),
    # ------------------------------------------------------------------
    # Phenom — white-label heavy, careers.{company}.com
    # API: api.phenom.com (OAuth-gated; DOM fingerprint preferred)
    # DOM signals: ph-/ phw- class prefixes
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.PHENOM,
        url_patterns=[
            r"phenom\.com",
            r"phenompeople\.com",
        ],
        dom_selectors=[
            r'class=[\'"][^\'"]*(ph-|phw-)',
            r"phenom-job",
            r"phenom\.com",
            r"phenompeople\.com",
            r"data-phenom",
        ],
        js_globals=["Phenom", "PhenomPeople", "phenom"],
        api_hints=[
            # OAuth-gated; provided for reference only
            "https://api.phenom.com/jobs-api/v1/jobs",
        ],
    ),
    # ------------------------------------------------------------------
    # Eightfold — {slug}.eightfold.ai/careers  /  white-label
    # No public API; scrape HTML.  DOM: eightfold-/ efai- prefixes
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.EIGHTFOLD,
        url_patterns=[
            r"\.eightfold\.ai",
        ],
        dom_selectors=[
            r'class=[\'"][^\'"]*(eightfold-|efai-)',
            r"eightfold\.ai",
            r"data-eightfold",
        ],
        js_globals=["Eightfold", "eightfold", "efai"],
        api_hints=[],  # No public API
    ),
    # ------------------------------------------------------------------
    # SAP SuccessFactors — heavily white-labeled
    # API: OData v2  /odata/v2/JobRequisition?$format=json
    # Hidden sitemap at /sitemal.xml (documented typo in SAP's platform)
    # Reference: career.sap.com
    # ------------------------------------------------------------------
    ATSFingerprint(
        provider=ATSProvider.SAP_SUCCESSFACTORS,
        url_patterns=[
            r"successfactors\.com",
            r"career\.sap\.com",
            r"sap\.com/careers",
        ],
        dom_selectors=[
            r"successfactors",
            r"sfsf-",
            r"saphr",
            r"data-sap-",
            r"SAP SuccessFactors",
        ],
        js_globals=["SuccessFactors", "SFSF", r"sap\.sf"],
        api_hints=[
            "/odata/v2/JobRequisition?$format=json",
            "/sitemal.xml",  # SAP's documented (typo) sitemap endpoint
        ],
    ),
]

# Lookup by provider for O(1) access
FINGERPRINT_BY_PROVIDER: dict[ATSProvider, ATSFingerprint] = {
    fp.provider: fp for fp in FINGERPRINTS
}
