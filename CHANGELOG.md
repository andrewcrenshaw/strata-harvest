# Changelog

All notable changes to **strata-harvest** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Ashby tenant slug resolver** (ENH-04 / PCC-1736): Ashby-hosted career pages on
  custom domains (e.g. Granola AI, Notion, Loom, Retool) now return >0 jobs.
  `AshbyParser.extract_slug_from_html()` scans the fetched HTML for the
  `organizationHostedJobsPageName` JSON config key (primary) or an
  `ashbyhq.com/job-board/<slug>` URL (fallback), then queries the Ashby GraphQL
  API with the resolved slug.  When no slug can be found a warning is logged and
  an empty result is returned (unchanged prior behaviour).

## [0.1.1] - 2026-03-30

### Changed

- Documented **pre-alpha / development** status in README; PyPI classifier set to **Development Status :: 2 - Pre-Alpha**. Version remains **0.x** until APIs and quality bar are stable.

## [0.1.0] - 2026-03-30

### Added

- Initial public release on PyPI.
- Public API: `harvest()`, `create_crawler()`, and core models (`JobListing`, `ScrapeResult`, `ATSInfo`).
- ATS-oriented parsers (Greenhouse, Lever, Ashby, and stubs for Workday/iCIMS) with HTTP fetching, rate limiting, and optional LLM fallback via the `llm` extra (`pip install strata-harvest[llm]`).

[0.1.1]: https://github.com/andrewcrenshaw/strata-harvest/releases/tag/v0.1.1
[0.1.0]: https://github.com/andrewcrenshaw/strata-harvest/releases/tag/v0.1.0
