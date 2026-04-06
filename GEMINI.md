# GEMINI.md — Strata Harvest

> Antigravity-specific config for the `strata-harvest` repository.

---

## Project Identity

**strata-harvest** is a career page scraping library — a Python package (`src/strata_harvest/`) that fetches job listings from ATS providers (Greenhouse, Lever, Ashby, Workday, iCIMS) and public career pages. Used by `strata` as a dependency.

---

## Shared PDT Infrastructure

This project belongs to the Andrew Crenshaw development workspace. All backlog, session, and ticket operations use the shared PDT API:

| | |
|-|-|
| **API** | `http://localhost:5176/api` |
| **Start** | `cd ~/Development/autogenous-synthesis && npm run start:dashboard` |
| **Health check** | `curl http://localhost:5176/api` |
| **Docs** | `~/Development/autogenous-synthesis/docs/guides/` |

### Create a Ticket

```bash
curl -s -X POST http://localhost:5176/api/backlog \
  -H "Content-Type: application/json" \
  -d '{
    "title": "...",
    "type": "implementation",
    "priority": "high",
    "description": "...",
    "repo": "strata-harvest"
  }'
```

---

## Session Lifecycle

Use `/session` (available at `~/Development/.agents/workflows/session.md`).
Scratchpads live in **this repo**: `.agent-data/scratchpads/active/` and `archive/YYYY-MM/`

---

## Project-Specific Rules

- **Language/stack:** Python 3.12+, httpx, BeautifulSoup4, pytest
- **Package layout:** `src/strata_harvest/` — use `pip install -e .` or `uv pip install -e .`
- **Tests:** `pytest tests/`
- **File writes:** Native Antigravity tools are fine

---

## Deeper Reference

- KIs: `~/.gemini/antigravity/knowledge/` (auto-injected)
- Bootstrap: `~/Development/autogenous-synthesis/docs/guides/AGENT_BOOTSTRAP.md`
