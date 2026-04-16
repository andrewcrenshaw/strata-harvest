# ADR-002: Tiered Fetcher and Extraction Pipeline

- **Status:** Accepted
- **Date:** 2026-04-16
- **Deciders:** Andrew Crenshaw (operator)
- **Tracking tickets:** PCC-1946, PCC-1947, PCC-1948, PCC-1949, PCC-1950, PCC-1956

## Context

Strata-harvest had two compounding problems:

1. **Ad-hoc fetcher choices.** `scrapling` was adopted without integration. A dev agent proposed `Playwright` as a general-purpose fetcher. Neither decision was escalated to the operator. No formal stance existed on when a browser is justified.
2. **Wrong-page false positives.** URL regex + loose DOM probe let blog posts on `/careers` paths, investor-relations pages, and wrong-ATS-slug URLs (Ironclad) pass as valid careers pages. LLM fallback swallowed this silently.

Without a formal tier model, every new "this page didn't scrape" incident tempted a heavier default — the path that ends in "Playwright for everything" and a 10× cost increase.

## Decision

The fetcher and extraction stack is **strictly tiered**. Each tier has a cost and a justification. Escalation requires an explicit reason code logged to the scrape audit record.

```
Tier 0  ATS JSON/GraphQL API (when detected)           httpx async
Tier 1  Static HTML with known ATS shell               httpx + selectolax
Tier 2  Tier-1 403 or cloudflare-challenge body        curl_cffi (chrome124 impersonation)
Tier 3  Tier-2 still blocked / dynamic content         scrapling StealthyFetcher
Tier 4  Heavy SPA / conversational (iCIMS, Paradox)    Crawl4AI (wraps Playwright)
Tier 5  Deterministic extraction incomplete            extruct → trafilatura → local Qwen via Ollama
```

Rules:

- **No tier skipping.** A page that works at tier 1 does not get promoted to tier 3+.
- **Escalation is logged.** Every tier promotion emits `{url, from_tier, to_tier, reason_code}` to the scrape audit.
- **Playwright is tier 4 only.** Crawl4AI wraps Playwright; that is the supported browser path. No direct Playwright integration.
- **LLM is tier 5 and local-first.** Default model: `qwen2.5:7b-instruct` via Ollama + `format=json` + `instructor`. Hosted Gemini only when Ollama unreachable AND `HOSTED_LLM_FALLBACK_ENABLED=1`.
- **A `CareersPageValidator` runs before tier 0.** Rejects blog/IR/archived/noindex pages with structured reason codes before any fetcher tier runs.
- **Dependencies stay in extras.** `[stealth]` (scrapling, curl_cffi), `[extract]` (extruct, trafilatura, selectolax), `[local-llm]` (instructor, ollama/litellm-ollama), `[browser]` (Crawl4AI — existing). Base install is httpx + stdlib.

## Consequences

### Positive

- "Wrong page" class becomes observable (reject reason codes → `wrong_page_rate` metric).
- Most career pages stay on tier 0 or 1; browser costs contained to the minority that actually need them.
- LLM token consumption drops 67–98% on the remaining cases (AXE/Firecrawl literature).
- Local LLM default removes hosted-API cost and privacy concerns for the common case.
- Operator retains explicit control: tier escalations are auditable, no silent drift to heavier defaults.

### Negative

- More moving parts. Four optional extras to manage (`[stealth]`, `[extract]`, `[local-llm]`, `[browser]`).
- Tier escalation logic adds complexity to `crawler.py` orchestrator.
- Local-LLM path requires Ollama running; tests must skip cleanly when unavailable.
- Operators must run a 50-sample validation set before flipping `DISCOVERY` to local-eligible in strata (PCC-1951).

### Neutral

- Playwright may already be installed in the strata venv as a contingency (scratchpad `agent-ironclad-rippling-scraping-fix-2026-04-16.md`). That install is superfluous under this ADR — Crawl4AI installs its own Playwright. The contingency should be removed as part of PCC-1956 cleanup.

## Alternatives considered

1. **Playwright-default (rejected).** ~5–10× RAM, ~10× latency vs httpx. Over-provisioned for the ~80% of career pages that don't need JS. The dev agent's instinct that *some* pages need a browser is correct — this ADR keeps that capability at tier 4.
2. **Selenium (rejected).** Legacy, slower than Playwright, worse anti-bot. No reason to prefer it.
3. **ScrapingBee / ZenRows / Apify managed services (rejected).** Adds external dependency and per-request cost. Retained as a future escape hatch if tiers 0–4 prove insufficient at scale.
4. **Hosted LLM default with local opt-in (rejected).** Contrary to operator preference; cost and privacy concerns.
5. **No validator — rely on downstream heal (rejected).** Post-hoc healing is more expensive than preventing wrong pages at the gate, and the `exa_heal` fallback (PCC-1807) should be rare, not routine.

## Appendix A: Stealth-mode legal and ethical posture

Tiers 2 (`curl_cffi` impersonation) and 3 (`scrapling` StealthyFetcher) intentionally match browser fingerprints (JA3/HTTP2/TLS) to avoid naive WAF blocks. This raises questions that belong in the architectural record.

### Baseline — why this is low-risk for strata-harvest

- **Target scope is narrow:** public, indexable career boards intended for search-engine and applicant consumption. Not private dashboards, not logged-in content, not personal data.
- **Legal baseline (US):** *hiQ Labs v. LinkedIn* (9th Cir. 2019, 2022) held that scraping publicly-available data does not violate the CFAA. The 2022 hiQ settlement resolved that specific dispute but did not reverse the public-data holding.
- **Typical ATS TOS:** Greenhouse, Lever, Ashby, SmartRecruiters, and Workday expose public boards endpoints explicitly for third-party consumption and/or Google Jobs indexing.

### Required operational norms (enforced in code)

1. **Identify in User-Agent even when impersonating TLS.** Stealth is for fingerprint-matching, not identity concealment. Default UA format: `strata-harvest/{version} (+{contact_url})`. Never ship an empty or browser-mimicking UA.
2. **Never scrape behind authentication, login, or clickwrap.** No cookie replay against auth-gated dashboards. No credential storage for scraped sites.
3. **Respect `robots.txt`** even at tiers 2+ (`utils/robots.py`). The API-first ATS bypass (Greenhouse/Lever/Ashby) is the only exception and only applies to their documented public endpoints.
4. **Respect explicit deny responses.** A hard 403 with an "access denied" body, a Cloudflare challenge that declines to serve content after tier 3, or a legal takedown notice → do not retry, log to audit, stop.
5. **No PII handling.** Career pages rarely include candidate PII; if a page surfaces applicant info, abort and log.
6. **Per-domain rate ceiling respected across stealth tiers.** The existing `PerDomainRateLimiterRegistry` applies to tier-2 and tier-3 fetches — stealth does not grant permission to hit harder.

### Gray zones and how we stay out of them

- **Cloudflare/Akamai/DataDome challenge pages.** Tier 2 (`curl_cffi`) and tier 3 (`scrapling`) may pass challenges that tier 1 (`httpx`) does not. That is acceptable when the operator is serving a public careers board and the challenge is default-deployed anti-bot. It is **not** acceptable when the challenge is specifically targeting our UA or IP (we've been identified and asked to leave) — in that case, honor the signal.
- **TOS that prohibit automated access.** ATS-hosted public boards generally permit it; custom careers pages on corporate domains may have clickwrap-equivalent terms. We treat the schema.org/JobPosting markup (which is published for indexing) as implicit consent for structured-data extraction.
- **CAPTCHA solving services.** Out of scope. If a page requires CAPTCHA to view, tier 4 (Crawl4AI/Playwright) may trigger it — do not integrate paid CAPTCHA solvers without a separate ADR.

### Escalation

Any of the following triggers a pause and operator review, not a code retry:
- Cease-and-desist or legal takedown notice received for any target domain
- Sustained 403 + challenge-body response for the same domain across >3 sweeps despite tier escalation
- Site owner contacts us via the UA contact URL requesting removal

When in doubt: stop scraping that site, remove it from targets, document the decision in the scrape audit log.

---

## References

- ADR-001 (strata multi-agent data governance) — external, see [autogenous-synthesis](https://github.com/andrewcrenshaw/autogenous-synthesis/blob/main/docs/architecture/ADR-001-STRATA-MULTI-AGENT-DATA-GOVERNANCE.md)
- [ARCHITECTURE_ADVISORY_2026-04-16.md](../ARCHITECTURE_ADVISORY_2026-04-16.md) — full research and source citations
- PCC-1785 (seed_url schema) • PCC-1807 (Exa-based healing)
- hiQ Labs v. LinkedIn, 31 F.4th 1180 (9th Cir. 2022) — public-data CFAA holding
