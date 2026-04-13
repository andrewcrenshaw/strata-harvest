# Changelog

All notable changes to **strata-harvest** are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.5] - 2026-04-10

### Added

- **Exa AI discovery module** (`strata_harvest.discovery.exa_discovery`, PCC-1807):
  New `async find_career_page(company_name, *, exa_api_key)` function that queries
  Exa's semantic search for a company's current career page URL, scoring results by
  ATS signal strength (Greenhouse, Lever, Ashby, SmartRecruiters, etc.) and
  returning the highest-confidence URL above a configurable threshold.
  Activated via the new `exa` optional extra: `pip install strata-harvest[exa]`.
- **`exa` optional dependency**: `exa-py>=1.0` added to `pyproject.toml`
  `[project.optional-dependencies]`.


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

## [0.1.2] - 2026-04-09
### Fixed
- Ashby ATS tenant slug resolver validation. Uses fallback url parsing and GraphQL schema validation correctly.
