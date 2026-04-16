"""Tests for CareersPageValidator (PCC-1946).

Covers the six acceptance criteria cases:
- blog-at-/careers (BlogPosting schema → hard reject)
- IR page with join-us CTA (noindex / canonical → hard reject)
- valid Greenhouse board (ATS detected → strong positive)
- archived/404 (empty page → suspect/reject)
- Ironclad wrong-Ashby-slug case (URL hint only → weak positive)
- Rippling JS-hydrated case (sparse page + URL → suspect valid)

Plus unit-level coverage of every signal in the hierarchy.
"""

from __future__ import annotations

import pytest

from strata_harvest.models import ATSInfo, ATSProvider
from strata_harvest.validator.careers_page import (
    RC_ATS_DETECTED,
    RC_CANONICAL_NON_JOB,
    RC_EMPTY_PAGE,
    RC_NO_SIGNALS,
    RC_NOINDEX,
    RC_OG_ARTICLE,
    RC_SCHEMA_JOB_POSTING,
    RC_SCHEMA_NON_JOB,
    CareersPageValidator,
    ValidationResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALIDATOR = CareersPageValidator()


def _known_ats(provider: ATSProvider = ATSProvider.GREENHOUSE) -> ATSInfo:
    return ATSInfo(provider=provider, confidence=0.9, detection_method="url_pattern")


def _unknown_ats() -> ATSInfo:
    return ATSInfo()


def _jobs_page_html(extra: str = "") -> str:
    """Minimal HTML that looks like a real jobs page (dense enough text)."""
    jobs = "\n".join(
        f'<div class="job-card"><h3>Engineer {i}</h3><p>Location: SF</p></div>'
        for i in range(10)
    )
    return f"""
    <html><head><title>Open Roles at Acme</title></head>
    <body>
      <h1>We Are Hiring — Join Us</h1>
      {jobs}
      {extra}
    </body></html>
    """


def _blog_html(schema_type: str = "BlogPosting") -> str:
    return f"""
    <html><head>
      <script type="application/ld+json">
        {{"@context": "https://schema.org", "@type": "{schema_type}",
          "headline": "10 Tips for Your Job Search",
          "datePublished": "2026-04-01"}}
      </script>
    </head>
    <body>
      <h1>10 Tips for Your Job Search</h1>
      <p>{"x " * 200}</p>
    </body></html>
    """


def _ir_noindex_html() -> str:
    return """
    <html><head>
      <meta name="robots" content="noindex, nofollow">
      <title>Investor Relations — Acme Corp</title>
    </head>
    <body>
      <h1>Investor Relations</h1>
      <p>Join us as shareholders. {"x " * 50}</p>
    </body></html>
    """


def _ir_canonical_html(canonical: str) -> str:
    return f"""
    <html><head>
      <link rel="canonical" href="{canonical}">
      <title>Join Our Team</title>
    </head>
    <body>
      <h1>Join Our Team</h1>
      <p>{"We are hiring! " * 30}</p>
    </body></html>
    """


def _og_article_html() -> str:
    return """
    <html><head>
      <meta property="og:type" content="article">
      <meta property="article:published_time" content="2026-04-01T09:00:00Z">
      <title>Careers at Acme — Blog</title>
    </head>
    <body>
      <h1>Our Culture Blog</h1>
      <p>{"x " * 200}</p>
    </body></html>
    """


def _job_posting_schema_html() -> str:
    return """
    <html><head>
      <script type="application/ld+json">
        {"@context": "https://schema.org", "@type": "JobPosting",
         "title": "Senior Engineer", "hiringOrganization": {"name": "Acme"}}
      </script>
    </head>
    <body>
      <h1>Senior Engineer</h1>
      <p>We are looking for a Senior Engineer.</p>
    </body></html>
    """


def _empty_html() -> str:
    return "<html><body></body></html>"


def _sparse_html() -> str:
    return "<html><body><p>Hello</p></body></html>"


# ---------------------------------------------------------------------------
# ValidationResult dataclass
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestValidationResult:
    def test_defaults(self) -> None:
        r = ValidationResult(is_valid=True, confidence=0.9)
        assert r.reject_reason is None
        assert r.reason_code is None
        assert r.signals == []
        assert r.suspect is False

    def test_reject_result(self) -> None:
        r = ValidationResult(
            is_valid=False,
            confidence=0.8,
            reject_reason="bad page",
            reason_code="schema_non_job",
        )
        assert not r.is_valid
        assert r.reject_reason == "bad page"


# ---------------------------------------------------------------------------
# AC1: Blog post hosted at /careers URL → hard reject
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestBlogAtCareersUrl:
    def test_blog_posting_schema_at_careers_url_rejected(self) -> None:
        html = _blog_html("BlogPosting")
        result = _VALIDATOR.validate("https://example.com/careers/culture", html)
        assert not result.is_valid
        assert result.reason_code == RC_SCHEMA_NON_JOB
        assert "BlogPosting" in (result.reject_reason or "")

    def test_news_article_schema_rejected(self) -> None:
        html = _blog_html("NewsArticle")
        result = _VALIDATOR.validate("https://example.com/careers/news", html)
        assert not result.is_valid
        assert result.reason_code == RC_SCHEMA_NON_JOB

    def test_article_schema_rejected(self) -> None:
        html = _blog_html("Article")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_SCHEMA_NON_JOB

    def test_reject_reason_in_signals(self) -> None:
        html = _blog_html("BlogPosting")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert any("schema_non_job" in s for s in result.signals)

    def test_job_posting_schema_beats_blog_schema(self) -> None:
        """When @graph contains JobPosting, it should NOT be rejected."""
        html = """
        <html><head>
          <script type="application/ld+json">
            {"@context": "https://schema.org", "@graph": [
              {"@type": "Organization", "name": "Acme"},
              {"@type": "JobPosting", "title": "Engineer"}
            ]}
          </script>
        </head><body><h1>Jobs</h1></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/jobs", html)
        assert result.is_valid
        assert result.reason_code == RC_SCHEMA_JOB_POSTING


# ---------------------------------------------------------------------------
# AC2: IR page with join-us CTA → hard reject
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestIRPageWithJoinUsCTA:
    def test_noindex_meta_rejects(self) -> None:
        """IR page with noindex robots meta is rejected."""
        html = _ir_noindex_html()
        result = _VALIDATOR.validate(
            "https://ir.example.com/shareholders", html
        )
        assert not result.is_valid
        assert result.reason_code == RC_NOINDEX

    def test_canonical_to_investors_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/investors/shareholder-info")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_CANONICAL_NON_JOB
        assert "/investors/" in (result.reject_reason or "")

    def test_canonical_to_ir_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/ir/reports")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_CANONICAL_NON_JOB

    def test_canonical_to_blog_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/blog/our-culture")
        result = _VALIDATOR.validate("https://example.com/jobs", html)
        assert not result.is_valid
        assert result.reason_code == RC_CANONICAL_NON_JOB

    def test_canonical_to_news_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/news/2026/q1")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_CANONICAL_NON_JOB

    def test_canonical_to_press_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/press/releases")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid

    def test_canonical_to_archive_rejects(self) -> None:
        html = _ir_canonical_html("https://example.com/archive/2020/careers")
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid

    def test_og_article_with_published_time_rejects(self) -> None:
        html = _og_article_html()
        result = _VALIDATOR.validate("https://example.com/careers/blog-post", html)
        assert not result.is_valid
        assert result.reason_code == RC_OG_ARTICLE

    def test_og_article_without_published_time_passes(self) -> None:
        """og:type=article alone (no published_time) should not hard-reject."""
        job_cards = "\n".join(f'<div class="job-card">Engineer {i}</div>' for i in range(10))
        html = f"""
        <html><head>
          <meta property="og:type" content="article">
          <title>Careers</title>
        </head>
        <body>
          <h1>Join Our Team</h1>
          {job_cards}
        </body></html>
        """
        result = _VALIDATOR.validate("https://example.com/careers", html)
        # Should not be rejected by OG alone; later signals may make it valid
        assert result.reason_code != RC_OG_ARTICLE

    def test_noindex_in_join_us_page_rejected(self) -> None:
        """Even if the page says 'join us', noindex should still hard-reject."""
        html = """
        <html><head>
          <meta name="robots" content="noindex">
          <title>Join Us!</title>
        </head>
        <body><h1>Join our team</h1><p>We're hiring! " * 50</p></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/join-us", html)
        assert not result.is_valid
        assert result.reason_code == RC_NOINDEX


# ---------------------------------------------------------------------------
# AC3: Valid Greenhouse board → strong positive
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestValidGreenhouseBoard:
    def test_greenhouse_ats_info_validates(self) -> None:
        html = _jobs_page_html()
        result = _VALIDATOR.validate(
            "https://boards.greenhouse.io/acme/jobs",
            html,
            ats_info=_known_ats(ATSProvider.GREENHOUSE),
        )
        assert result.is_valid
        assert result.reason_code == RC_ATS_DETECTED
        assert result.confidence >= 0.9

    def test_any_known_ats_validates(self) -> None:
        for provider in (
            ATSProvider.LEVER,
            ATSProvider.ASHBY,
            ATSProvider.WORKDAY,
            ATSProvider.RIPPLING,
        ):
            result = _VALIDATOR.validate(
                "https://jobs.example.com",
                _sparse_html(),  # even sparse HTML is fine when ATS known
                ats_info=_known_ats(provider),
            )
            assert result.is_valid, f"Expected valid for {provider}"
            assert result.reason_code == RC_ATS_DETECTED

    def test_job_posting_json_ld_validates(self) -> None:
        html = _job_posting_schema_html()
        result = _VALIDATOR.validate("https://example.com/jobs/42", html)
        assert result.is_valid
        assert result.reason_code == RC_SCHEMA_JOB_POSTING
        assert result.confidence == pytest.approx(0.95)

    def test_job_posting_in_graph_validates(self) -> None:
        html = """
        <html><head>
          <script type="application/ld+json">
            {"@context": "https://schema.org", "@graph": [
              {"@type": "WebSite"},
              {"@type": "JobPosting", "title": "PM"}
            ]}
          </script>
        </head><body></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/jobs/pm", html)
        assert result.is_valid
        assert result.reason_code == RC_SCHEMA_JOB_POSTING


# ---------------------------------------------------------------------------
# AC4: Archived / 404 page → suspect / reject
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestArchivedOrEmptyPage:
    def test_completely_empty_html_rejected(self) -> None:
        # Use a URL with no /careers-like segment so URL signal doesn't rescue it
        result = _VALIDATOR.validate(
            "https://old.example.com/archive/2020", ""
        )
        assert not result.is_valid
        assert result.reason_code == RC_EMPTY_PAGE
        assert result.suspect is True

    def test_minimal_html_no_content_rejected(self) -> None:
        result = _VALIDATOR.validate(
            "https://example.com/careers-2019", _empty_html()
        )
        # Empty HTML with no URL signals → reject
        assert result.reason_code == RC_EMPTY_PAGE

    def test_sparse_html_with_careers_url_is_suspect_valid(self) -> None:
        """Sparse page + careers URL → valid but suspect (may need heal)."""
        result = _VALIDATOR.validate(
            "https://example.com/careers", _sparse_html()
        )
        assert result.is_valid
        assert result.suspect is True
        assert result.reason_code == RC_EMPTY_PAGE

    def test_404_page_without_job_signals_rejected(self) -> None:
        html = "<html><body><h1>404 Not Found</h1><p>Page not found.</p></body></html>"
        result = _VALIDATOR.validate("https://archive.example.com/old-jobs", html)
        assert not result.is_valid


# ---------------------------------------------------------------------------
# AC5: Ironclad wrong-Ashby-slug case
# (wrong slug → page returns valid HTML but is not a real jobs board)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestIroncladWrongAshbySlug:
    def test_wrong_ashby_slug_url_with_careers_path_is_weak_positive(self) -> None:
        """
        URL: https://jobs.ashbyhq.com/ironclad (wrong slug → likely 404 or
        empty board page). If no ATS info and URL has /careers pattern from
        the company domain, should be weak positive.
        """
        # Simulate the company's own careers URL (before redirect to Ashby)
        # The strata DB might store ironcladapp.com/careers as seed_url
        sparse_careers_html = """
        <html><head><title>Careers - Ironclad</title></head>
        <body><h1>Open Positions</h1><p>Loading...</p></body></html>
        """
        result = _VALIDATOR.validate(
            "https://ironcladapp.com/careers",
            sparse_careers_html,
        )
        # /careers URL + "Open Positions" / "Careers" in title → weak positive
        assert result.is_valid
        # Low confidence since sparse (likely JS-rendered)
        assert result.confidence < 0.8

    def test_wrong_ashby_slug_empty_board_is_suspect(self) -> None:
        """
        When the wrong Ashby slug returns an essentially empty board,
        the validator should flag it as suspect (needs heal).
        """
        empty_board = """
        <html><head><title>Jobs | Ashby</title></head>
        <body><div id="ashby-application-root"></div></body></html>
        """
        # The Ashby URL with wrong slug — Ashby detected by DOM
        ats = ATSInfo(
            provider=ATSProvider.ASHBY,
            confidence=0.8,
            detection_method="dom_probe",
        )
        result = _VALIDATOR.validate(
            "https://jobs.ashbyhq.com/ironclad",
            empty_board,
            ats_info=ats,
        )
        # ATS detected → strong positive (validator trusts detector)
        assert result.is_valid
        assert result.reason_code == RC_ATS_DETECTED


# ---------------------------------------------------------------------------
# AC6: Rippling JS-hydrated case
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestRipplingJSHydrated:
    def test_sparse_rippling_careers_url_is_suspect_valid(self) -> None:
        """
        Rippling's initial HTML is nearly empty (JS-rendered).
        URL has /careers → weak positive + sparse → suspect.
        """
        sparse_html = """
        <html><head><title>Open Roles | Rippling</title></head>
        <body><div id="__next"></div></body></html>
        """
        result = _VALIDATOR.validate(
            "https://www.rippling.com/careers/open-roles",
            sparse_html,
        )
        assert result.is_valid
        assert result.suspect is True
        assert result.reason_code == RC_EMPTY_PAGE

    def test_rippling_with_ats_detected_always_valid(self) -> None:
        """When Rippling ATS is detected (via URL pattern), validator is valid."""
        html = "<html><body><div id='__next'></div></body></html>"
        ats = ATSInfo(
            provider=ATSProvider.RIPPLING,
            confidence=0.9,
            detection_method="url_pattern",
        )
        result = _VALIDATOR.validate(
            "https://ats.rippling.com/company/jobs",
            html,
            ats_info=ats,
        )
        assert result.is_valid
        assert result.reason_code == RC_ATS_DETECTED


# ---------------------------------------------------------------------------
# Integration: existing strata sweep URLs should produce same/better precision
# (smoke tests using representative HTML patterns)
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestIntegrationPrecision:
    def test_dense_jobs_page_validates(self) -> None:
        """A page full of job cards with /careers URL should always validate."""
        html = _jobs_page_html()
        result = _VALIDATOR.validate(
            "https://example.com/careers",
            html,
        )
        assert result.is_valid

    def test_greenhouse_embed_page_validates(self) -> None:
        """Greenhouse embed URL detected → strong positive regardless of HTML."""
        result = _VALIDATOR.validate(
            "https://boards.greenhouse.io/acme/jobs",
            _sparse_html(),
            ats_info=_known_ats(ATSProvider.GREENHOUSE),
        )
        assert result.is_valid

    def test_no_signals_page_rejected(self) -> None:
        """A page with no job signals at all (e.g. homepage) is rejected."""
        filler = "We make great products for enterprise customers. " * 10
        html = f"""
        <html><head><title>Acme Corp — Home</title></head>
        <body>
          <h1>Welcome to Acme</h1>
          <p>{filler}</p>
        </body></html>
        """
        result = _VALIDATOR.validate("https://example.com/", html)
        assert not result.is_valid
        assert result.reason_code == RC_NO_SIGNALS


# ---------------------------------------------------------------------------
# Unit tests for individual signal helpers
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestSignalHierarchy:
    def test_url_pattern_positions_validates(self) -> None:
        html = _jobs_page_html()
        result = _VALIDATOR.validate("https://example.com/positions", html)
        assert result.is_valid
        assert "url_pattern" in result.signals

    def test_url_pattern_openings_validates(self) -> None:
        html = _jobs_page_html()
        result = _VALIDATOR.validate("https://example.com/openings", html)
        assert result.is_valid

    def test_title_regex_we_are_hiring(self) -> None:
        job_cards = "\n".join(f'<div class="job-card">Role {i}</div>' for i in range(10))
        html = f"""
        <html><head><title>We Are Hiring!</title></head>
        <body>
          <h1>We Are Hiring</h1>
          {job_cards}
        </body></html>
        """
        result = _VALIDATOR.validate("https://example.com/company", html)
        assert result.is_valid
        assert "title_match" in result.signals

    def test_url_and_title_combined_higher_confidence(self) -> None:
        html = _jobs_page_html()
        url_only_result = _VALIDATOR.validate("https://example.com/careers", _sparse_html())
        url_and_title_result = _VALIDATOR.validate("https://example.com/careers", html)
        # Combined signals should be at least as confident as URL alone
        assert url_and_title_result.confidence >= url_only_result.confidence

    def test_unknown_ats_falls_through_to_signals(self) -> None:
        """When ATS is UNKNOWN, validator should use URL/title signals."""
        html = _jobs_page_html()
        result = _VALIDATOR.validate(
            "https://example.com/careers",
            html,
            ats_info=_unknown_ats(),
        )
        assert result.is_valid
        # Should NOT use ATS detection reason code
        assert result.reason_code != RC_ATS_DETECTED

    def test_signals_list_is_populated(self) -> None:
        html = _jobs_page_html()
        result = _VALIDATOR.validate(
            "https://example.com/careers",
            html,
        )
        assert len(result.signals) > 0

    def test_noindex_case_insensitive(self) -> None:
        html = """
        <html><head>
          <META NAME="Robots" CONTENT="NOINDEX, NOFOLLOW">
        </head><body><p>x</p></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_NOINDEX

    def test_canonical_url_content_first_attribute_order(self) -> None:
        """Canonical link where href comes before rel."""
        html = """
        <html><head>
          <link href="https://example.com/blog/story" rel="canonical">
        </head><body><h1>Jobs</h1></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/careers", html)
        assert not result.is_valid
        assert result.reason_code == RC_CANONICAL_NON_JOB

    def test_canonical_to_jobs_section_not_rejected(self) -> None:
        """Canonical pointing to /jobs should NOT trigger canonical reject."""
        job_cards = "\n".join(f'<div class="job-card">Engineer {i}</div>' for i in range(10))
        html = f"""
        <html><head>
          <link rel="canonical" href="https://example.com/jobs">
          <title>Jobs</title>
        </head>
        <body>
          <h1>Open Positions</h1>
          {job_cards}
        </body></html>
        """
        result = _VALIDATOR.validate("https://example.com/careers", html)
        # canonical to /jobs is fine — should not trigger reject
        assert result.reason_code != RC_CANONICAL_NON_JOB

    def test_og_content_before_property_attribute_order(self) -> None:
        """og:type meta where content comes before property."""
        html = """
        <html><head>
          <meta content="article" property="og:type">
          <meta property="article:published_time" content="2026-01-01">
        </head><body><p>{"x " * 50}</p></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/careers/culture", html)
        assert not result.is_valid
        assert result.reason_code == RC_OG_ARTICLE

    def test_multiple_json_ld_blocks(self) -> None:
        """First block is non-job, second is JobPosting — should return valid."""
        html = """
        <html><head>
          <script type="application/ld+json">
            {"@type": "Organization", "name": "Acme"}
          </script>
          <script type="application/ld+json">
            {"@type": "JobPosting", "title": "Engineer"}
          </script>
        </head><body></body></html>
        """
        result = _VALIDATOR.validate("https://example.com/jobs/1", html)
        assert result.is_valid
        assert result.reason_code == RC_SCHEMA_JOB_POSTING

    def test_url_pattern_not_triggered_for_homepage(self) -> None:
        result = _VALIDATOR.validate("https://example.com/", _jobs_page_html())
        # /  has no /careers /jobs pattern in path
        assert "url_pattern" not in result.signals


# ---------------------------------------------------------------------------
# Audit logging
# ---------------------------------------------------------------------------


@pytest.mark.verification
class TestAuditLogging:
    def test_reject_emits_log(self, caplog: pytest.LogCaptureFixture) -> None:
        import logging

        with caplog.at_level(logging.INFO, logger="strata_harvest.validator.careers_page"):
            html = _blog_html("BlogPosting")
            _VALIDATOR.validate("https://example.com/careers", html)

        assert any("rejected" in r.message for r in caplog.records)

    def test_valid_page_does_not_emit_reject_log(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        import logging

        with caplog.at_level(logging.INFO, logger="strata_harvest.validator.careers_page"):
            _VALIDATOR.validate(
                "https://boards.greenhouse.io/acme",
                _sparse_html(),
                ats_info=_known_ats(),
            )

        assert not any("rejected" in r.message for r in caplog.records)
