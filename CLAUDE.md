# CLAUDE.md — Strata Harvest

> Inherits shared workspace context from `~/Development/CLAUDE.md`. This file adds `strata-harvest`-specific context only.

---

## Project Identity

**strata-harvest** is a career page scraping library. Python package at `src/strata_harvest/`. Provides ATS provider adapters (Greenhouse, Lever, Ashby, Workday, iCIMS) and HTML/JSON-LD parsing for job listings.

---

## Stack & Dev Commands

```bash
source .venv/bin/activate   # or: uv venv && source .venv/bin/activate
pip install -e .             # or: uv pip install -e .
pytest tests/ -v
pre-commit run --all-files
```

---

## This Repo's Backlog Tag

When creating tickets: `"repo": "strata-harvest"`

---

*Shared infrastructure (PDT API, session lifecycle, ticket creation) → `~/Development/CLAUDE.md`*
