# CLAUDE.md — Strata Harvest

> Inherits shared workspace context from `~/Development/CLAUDE.md`. This file adds `strata-harvest`-specific context only.

---

## Agent Memory (Framework-Wide)

Framework behavioral memories are stored at the workspace level: `~/.claude/projects/-Users-andrewcrenshaw-Development/memory/`

Key entries (shared across all Development projects):
- `session_lifecycle.md` — Mandatory register→context→claim→work→submit→reflect→archive→delete sequence
- `session_reflection_format.md` — Required DELETE payload with reflection.decisions[] — this is how decision_traces gets populated
- `routing_rules.md` — Strata external repos + subagent delegation thresholds (>3 reads → Explore subagent, 5+ files → parallel)

Project-specific memories (user_profile, project-level feedback) remain in autogenous-synthesis project memory.

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
