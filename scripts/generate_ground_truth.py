"""Generate ground-truth expected output files for test fixtures.

Runs each ATS parser against its fixture files and writes the parsed
JobListing data (minus raw_data) as JSON expected-output files.

Usage:
    python scripts/generate_ground_truth.py
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from strata_harvest.parsers.greenhouse import GreenhouseParser
from strata_harvest.parsers.lever import LeverParser
from strata_harvest.parsers.ashby import AshbyParser
from strata_harvest.models import JobListing

FIXTURES_ROOT = Path(__file__).resolve().parent.parent / "tests" / "fixtures"
EXPECTED_ROOT = FIXTURES_ROOT / "expected"


def job_to_dict(job: JobListing) -> dict:
    """Convert a JobListing to a dict suitable for ground truth (excludes raw_data)."""
    d = job.model_dump(mode="json")
    d.pop("raw_data", None)
    if d.get("url"):
        d["url"] = str(d["url"])
    return d


def generate_for_provider(
    provider_dir: str,
    parser,
    url_template: str,
    fixtures: list[str],
) -> None:
    """Run parser on each fixture and write expected output."""
    out_dir = EXPECTED_ROOT / provider_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    for fixture_name in fixtures:
        fixture_path = FIXTURES_ROOT / provider_dir / fixture_name
        if not fixture_path.exists():
            print(f"  SKIP {fixture_name} (not found)")
            continue

        content = fixture_path.read_text(encoding="utf-8")
        url = url_template.format(name=fixture_name.replace(".json", ""))
        listings = parser.parse(content, url=url)

        expected = [job_to_dict(j) for j in listings]
        out_path = out_dir / fixture_name
        out_path.write_text(
            json.dumps(expected, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"  {fixture_name} → {len(expected)} jobs")


def generate_career_page_expected() -> None:
    """Create ground-truth stubs for raw HTML career pages (no ATS parser).

    These fixtures exist for detector testing and LLM fallback validation.
    The ground truth captures what a human would expect to find.
    """
    out_dir = EXPECTED_ROOT / "career_pages"
    out_dir.mkdir(parents=True, exist_ok=True)

    pages = {
        "unknown_custom.json": [
            {
                "title": "Office Manager",
                "location": "Remote",
            },
            {
                "title": "Marketing Intern",
                "location": "New York",
            },
        ],
        "boutique_agency.json": [
            {
                "title": "Senior Copywriter",
                "location": "Portland, OR / Remote",
                "department": "Content Team",
                "employment_type": "Full-time",
                "salary_range": "$95,000 – $120,000/year",
            },
            {
                "title": "UX Designer",
                "location": "Remote (US)",
                "department": "Design Team",
                "employment_type": "Full-time",
            },
            {
                "title": "Project Coordinator",
                "location": "Portland, OR",
                "department": "Operations",
                "employment_type": "Part-time",
                "salary_range": "$28–$35/hour",
            },
        ],
        "local_restaurant_group.json": [
            {
                "title": "Executive Chef — Hearth Downtown",
                "location": "Minneapolis, MN",
                "employment_type": "Full-time",
                "salary_range": "$75,000 – $90,000/year + bonus",
            },
            {
                "title": "Front-of-House Manager — Vine St. Paul",
                "location": "St. Paul, MN",
                "employment_type": "Full-time",
            },
            {
                "title": "Line Cook — All Locations",
                "location": "Minneapolis or St. Paul, MN",
                "employment_type": "Full-time or Part-time",
                "salary_range": "$18–$24/hour depending on experience",
            },
        ],
    }

    for name, expected in pages.items():
        path = out_dir / name
        path.write_text(
            json.dumps(expected, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        print(f"  {name} → {len(expected)} jobs (human-annotated)")


def main() -> None:
    print("Generating ground truth expected output files...\n")

    print("Greenhouse API:")
    generate_for_provider(
        "greenhouse_api",
        GreenhouseParser(),
        "https://boards.greenhouse.io/{name}",
        ["acmecorp.json", "greenleaf.json", "megacorp.json"],
    )

    print("\nLever:")
    generate_for_provider(
        "lever",
        LeverParser(),
        "https://jobs.lever.co/{name}",
        ["lever_multi_postings.json", "lever_single_posting.json"],
    )

    print("\nAshby:")
    generate_for_provider(
        "ashby",
        AshbyParser(),
        "https://jobs.ashbyhq.com/{name}",
        ["ashby_job_board_response.json", "ashby_single_posting.json"],
    )

    print("\nCareer pages (human-annotated):")
    generate_career_page_expected()

    print("\nDone.")


if __name__ == "__main__":
    main()
