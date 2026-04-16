# Strata-Harvest Architecture Advisory — 2026-04-16

> **Status:** Advisory • **Author:** alex (with research delegated to Explore + general-purpose subagents) • **Scope:** strata-harvest + the strata discovery/sweep paths that feed it.
>
> Tracking tickets: **PCC-1946** through **PCC-1956**.

## 1. Problem statement

Strata is operational but surfaced two classes of harvest-layer problems:

1. **"Wrong page" false positives.** URLs treated as careers pages that weren't: stale redirects, blog posts matching `/careers`, investor-relations "join us" fluff, wrong-ATS-slug cases (Ironclad), zero-result JS-hydrated cases (Rippling pre-fix).
2. **Architectural drift without operator consultation.** `scrapling` was decided-on but never wired in. A dev agent now wants to add Playwright. Neither decision was escalated to the user. Stated preference: avoid agent-browser navigation except as a last resort; prefer deterministic scraping; use LLMs only when needed and prefer local models.

This document evaluates the current architecture against 2024–2026 state-of-the-art and recommends a tiered, structured-data-first pipeline with local-LLM default.

---

## 2. Current-state audit

### 2.1 What's good

- **API-first** for Greenhouse, Lever, Ashby, SmartRecruiters, Rippling — correct design choice.
- **Async httpx fetcher** with SSRF guard, retries, size limits, gzip/br/zstd decompression ([utils/http.py](src/strata_harvest/utils/http.py)).
- **Per-origin robots.txt caching**; global + per-domain token-bucket rate limiting.
- **Clean parser registry** and `ATSProvider` enum.
- **Crawl4AI used as *optional* SPA fallback**, not default path ([parsers/crawl4ai_extractor.py](src/strata_harvest/parsers/crawl4ai_extractor.py)).
- **Exa-based post-sweep healing** (PCC-1807) with tiered A/B/C strategy in [sweep/heal.py](../../strata/apps/api/sweep/heal.py).
- **seed_url field** (PCC-1785) designed specifically to separate stable canonical from volatile ATS endpoint — the right model.

### 2.2 What's broken or missing

| # | Gap | Evidence |
|---|-----|----------|
| 1 | No "is this a careers page?" validator. Detection = regex URL match + loose DOM probe. Blogs at `/careers/why-we-love-our-mission` pass. | [detector.py](src/strata_harvest/detector.py) |
| 2 | `scrapling` was adopted as a decision but never activated. Zero imports in `src/`. | grep confirms. |
| 3 | Playwright installed as contingency during Rippling fix but not integrated. | scratchpad `agent-ironclad-rippling-scraping-fix-2026-04-16.md:24` |
| 4 | No `curl_cffi` / TLS-impersonation tier. Plain httpx increasingly fails on sites with Cloudflare/Akamai/DataDome in 2026. | — |
| 5 | No `extruct` or structured-data-first extraction. Workday, iCIMS, Rippling each reimplement JSON-LD parsing with regex. | [parsers/workday.py](src/strata_harvest/parsers/workday.py), [parsers/icims.py](src/strata_harvest/parsers/icims.py), [parsers/rippling.py](src/strata_harvest/parsers/rippling.py) |
| 6 | LLM fallback sends raw cleaned HTML — no DOM pruning, no trafilatura/readability pre-pass. AXE-class token waste (literature reports 67–98% reduction possible). | [parsers/llm_fallback.py:254-273](src/strata_harvest/parsers/llm_fallback.py) |
| 7 | LLM path hardcodes `gemini/gemini-2.5-flash` in two places; local LLM isn't default and isn't tested. | [parsers/llm_fallback.py:28](src/strata_harvest/parsers/llm_fallback.py), [parsers/crawl4ai_extractor.py:73](src/strata_harvest/parsers/crawl4ai_extractor.py) |
| 8 | No `instructor`/Pydantic structured output. LLM output parsed ad-hoc. | — |
| 9 | ATS coverage gaps: Teamtailor, Recruitee, Pinpoint, Breezy, Phenom, Eightfold, SAP SuccessFactors. All detectable; most have public feeds. | — |
| 10 | Upstream `DiscoveryAgent` LLM generates `career_url_hints` with no structured validation. | [strata/apps/api/discovery/agent.py:183-202](../../strata/apps/api/discovery/agent.py) |
| 11 | `seed_url` rarely populated. `DISCOVERY` missing from `LOCAL_ELIGIBLE_STAGES` — always hits hosted Gemini. | [strata/apps/api/llm/config.py:67-79](../../strata/apps/api/llm/config.py) |

---

## 3. Target architecture

### 3.1 Tiered fetcher stack

Cheapest-first. Escalation requires an explicit reason code logged to the scrape audit record. Pages that work at tier 1 never touch tier 3+.

| Tier | Use when | Tool |
|------|----------|------|
| 0 | Known ATS with JSON/GraphQL API | httpx async |
| 1 | Static HTML with known ATS shell | httpx + selectolax |
| 2 | Tier-1 returned 403 or cloudflare-challenge body | `curl_cffi` (chrome124 impersonation) |
| 3 | Tier-2 still blocked / dynamic content | `scrapling` StealthyFetcher |
| 4 | Heavy SPA, conversational ATS (iCIMS, Paradox, Eightfold) | Crawl4AI (wraps Playwright) |
| 5 | Deterministic extraction still empty for key fields | Structured-data-first extraction → local LLM |

**Playwright sits at tier 4 only.** The dev agent's instinct that *some pages need a browser* is correct. Framing it as the default fetcher would be wrong — the ~80% of careers pages that work at tiers 0–2 must never pay browser costs (~5–10× RAM, ~10× latency vs httpx). The hybrid pattern (Playwright for login/cookies → httpx for data fetches, ~10× speedup) is the right compromise when a browser is actually required.

### 3.2 Careers-page validation (the "wrong page" fix)

Add a `CareersPageValidator` stage that runs **before** any harvesting and returns `(is_valid, confidence, reject_reason)`. Signal hierarchy:

1. **`schema.org/JobPosting` JSON-LD** via `extruct` → strongest positive.
2. **Hard rejects:** `BlogPosting`/`NewsArticle`/`Article` schema; `og:type=article` + `article:published_time`; `noindex` meta; `canonical` pointing to `/blog|/news|/press|/investors|/ir|/archive`.
3. **ATS detection hit** (detector.py positive) → strong positive.
4. **URL pattern** (`/careers`, `/jobs`, `/positions`) → weak positive.
5. **Title/H1 regex** ("careers", "open roles", "we're hiring", "join us") → weak positive.
6. **Empty-result detection:** page text < 200 chars or zero job-list signals → mark suspect, trigger heal.

**Every reject is logged with a structured reason code.** This is how the wrong-page class of bugs becomes observable (and gives ops the `wrong_page_rate` metric instead of anecdotal "we noticed X was broken" incidents).

### 3.3 Extraction pipeline (token-minimizing)

Before any LLM call:

1. **`extruct` pass** — JSON-LD / microdata / RDFa / OpenGraph. Replaces the regex JSON-LD extraction in Workday/iCIMS/Rippling. Most cases end here.
2. **CSS/XPath** against known ATS shells (selectolax; lxml where XPath needed).
3. **`trafilatura`** (or `resiliparse` for speed) to strip nav/footer/scripts/ads → Markdown.
4. **Only then LLM fallback**, with:
   - `instructor` + Pydantic `JobPosting` schema.
   - Ollama `format=json` (xgrammar-backed constrained decoding) on local Qwen 2.5-7B-Instruct.
   - Hosted Gemini only if local Ollama unreachable AND operator opted in via `HOSTED_LLM_FALLBACK_ENABLED=1`.

Expected impact: AXE/Firecrawl literature reports 67–98% token reduction with pruning; JSON-LD-first typically eliminates the LLM call entirely.

### 3.4 Local LLM

- **Default:** `qwen2.5:7b-instruct` via Ollama (loopback `localhost:11434`) + `format=json` + `instructor`.
- **Why Qwen 2.5:** explicitly trained for structured-output reliability; beats Llama 3.x on JSON extraction; runs comfortably on Mac.
- **Gotcha:** Qwen3 thinking mode has leaked reasoning tokens into JSON output via LiteLLM (documented in workspace memory [strata_llm_scoring_models.md](~/.claude/projects/-Users-andrewcrenshaw-Development/memory/strata_llm_scoring_models.md)). Disable thinking mode or use non-thinking variant.
- **Engine choice on Mac:** Ollama for dev (simplest), MLX or llama.cpp for production, vLLM only if moving to a GPU box. Ollama uses xgrammar under the hood — constrained decoding is free.

### 3.5 Upstream discovery hardening (strata, not strata-harvest)

- After `DiscoveryAgent._call_llm()` returns `career_url_hints`, pass each through `CareersPageValidator` **before** `role_precheck`. Reject non-careers URLs immediately instead of paying for role-keyword fetch.
- Auto-populate `seed_url` on import and after first successful harvest. Makes heal Option B actually useful.
- Add `DISCOVERY` to `LOCAL_ELIGIBLE_STAGES` after validating local Qwen 2.5-7B produces hints with precision within 5pp of hosted Gemini baseline (50-candidate eval set).

---

## 4. Deliberate non-recommendations

1. **Do not add Playwright as a general-purpose fetcher.** Tier 4 only. Most career pages don't need it.
2. **Do not keep Crawl4AI's `LLMExtractionStrategy` as the extraction fallback.** Its `RegexExtractionStrategy` / `JsonCssExtractionStrategy` are fine; the LLM strategy duplicates (worse) what the new tier-5 pipeline does.
3. **Do not build a "generic AI scraper."** Every page class that isn't a known ATS should have a named failure mode with a reason code, not a silent LLM call.
4. **Do not move dependencies to base install.** User decision: new libs go into extras — `[stealth]` (scrapling, curl_cffi), `[local-llm]` (instructor, ollama), `[extract]` (extruct, trafilatura, selectolax). Matches existing `[llm]`/`[browser]`/`[ocr]`/`[exa]` pattern; base install stays lean.

---

## 5. Tickets filed

| # | Ticket | Title | Priority | Repo |
|---|--------|-------|----------|------|
| T1 | **[PCC-1946](http://localhost:5175/backlog/PCC-1946)** | CareersPageValidator pre-harvest stage (extruct + hard-rejects) | critical | strata-harvest |
| T2 | **[PCC-1947](http://localhost:5175/backlog/PCC-1947)** | Activate scrapling as tier-3 StealthyFetcher | critical | strata-harvest |
| T3 | **[PCC-1948](http://localhost:5175/backlog/PCC-1948)** | curl_cffi tier-2 impersonation client | high | strata-harvest |
| T4 | **[PCC-1949](http://localhost:5175/backlog/PCC-1949)** | Replace regex JSON-LD with extruct in Workday/iCIMS/Rippling | high | strata-harvest |
| T5 | **[PCC-1950](http://localhost:5175/backlog/PCC-1950)** | Token-minimizing LLM pipeline: trafilatura + instructor + Qwen2.5-7B | critical | strata-harvest |
| T6 | **[PCC-1951](http://localhost:5175/backlog/PCC-1951)** | De-hardcode LLM; local default; DISCOVERY in LOCAL_ELIGIBLE_STAGES | high | strata |
| T7 | **[PCC-1952](http://localhost:5175/backlog/PCC-1952)** | Extend ATS coverage (Teamtailor/Recruitee/Pinpoint/Breezy/Phenom/Eightfold/SuccessFactors) | medium | strata-harvest |
| T8 | **[PCC-1953](http://localhost:5175/backlog/PCC-1953)** | Auto-populate seed_url; run validator upstream in DiscoveryAgent | high | strata |
| T9 | **[PCC-1954](http://localhost:5175/backlog/PCC-1954)** | sitemap.xml + lastmod; ETag/If-Modified-Since | medium | strata-harvest |
| T10 | **[PCC-1955](http://localhost:5175/backlog/PCC-1955)** | wrong_page_rate metric + reject-reason logging + alerts | medium | strata |
| T11 | **[PCC-1956](http://localhost:5175/backlog/PCC-1956)** | ADR-002: tiered fetcher model; Playwright is tier-4-only | medium | strata-harvest |

**Suggested execution order:** T1 → T4 (share extruct helper) → T2 → T3 → T5 → T8 (depends on T1) → T6 (depends on T5) → T7 → T11 → T9 → T10.

---

## 6. Sources

External research (2024–2026 state-of-the-art):

- **Careers-page detection:** [Google JobPosting docs](https://developers.google.com/search/docs/appearance/structured-data/job-posting); [schema.org JobPosting](https://schema.org/JobPosting); Zyte's `extruct` (actively maintained).
- **ATS coverage:** [fantastic.jobs ATS list](https://fantastic.jobs/article/ats-with-api); [Teamtailor API](https://docs.teamtailor.com/); [Phenom developer API](https://developer.phenom.com/apiDetail); [Eightfold API](https://apidocs.eightfold.ai/docs/getting-started).
- **Fetcher tooling:** [scrapling PyPI](https://pypi.org/project/scrapling/) (Feb 2026 release, 10.6k stars, 92% coverage); [ScrapingBee write-up](https://www.scrapingbee.com/blog/scrapling-adaptive-python-web-scraping/); [curl_cffi](https://github.com/lexiforest/curl_cffi); [Crawl4AI no-LLM strategies](https://docs.crawl4ai.com/extraction/no-llm-strategies/).
- **Parsing/extraction benchmarks:** [selectolax vs lxml](https://webscraping.fyi/lib/compare/python-lxml-vs-python-selectolax/); [Resiliparse benchmarks](https://chuniversiteit.nl/papers/comparison-of-web-content-extraction-algorithms); [Trafilatura evaluation](https://trafilatura.readthedocs.io/en/latest/evaluation.html).
- **LLM token minimization:** [Firecrawl LLM Extract launch](https://www.firecrawl.dev/blog/launch-week-i-day-6-llm-extract); [Firecrawl best-extraction-tools (AXE)](https://www.firecrawl.dev/blog/best-web-extraction-tools); [XGrammar paper](https://arxiv.org/pdf/2411.15100); [vLLM structured decoding](https://blog.vllm.ai/2025/01/14/struct-decode-intro.html).
- **Local LLMs:** [Red Hat vLLM vs llama.cpp](https://developers.redhat.com/articles/2025/09/30/vllm-or-llamacpp-choosing-right-llm-inference-engine-your-use-case); [Red Hat Ollama vs vLLM benchmark](https://developers.redhat.com/articles/2025/08/08/ollama-vs-vllm-deep-dive-performance-benchmarking); [Qwen 2.5 structured output](https://qwenlm.github.io/blog/qwen2.5-llm/).
- **Legal climate:** [hiQ v. LinkedIn (Wikipedia)](https://en.wikipedia.org/wiki/HiQ_Labs_v._LinkedIn); [ZwillGen retrospective](https://www.zwillgen.com/alternative-data/hiq-v-linkedin-wrapped-up-web-scraping-lessons-learned/).

Internal references:

- [strata-harvest CLAUDE.md](CLAUDE.md)
- [strata-harvest CHANGELOG.md](CHANGELOG.md)
- scratchpads: `agent-ironclad-rippling-scraping-fix-2026-04-16.md`, `bob-pcc-1807-2026-04-10.md`
- Memory: `strata_llm_scoring_models.md`, `strata_matching_scaling.md`
