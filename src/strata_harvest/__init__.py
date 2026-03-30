"""strata-harvest: Career page scraping and ATS parsing library."""

from strata_harvest.crawler import Crawler, create_crawler, harvest
from strata_harvest.detector import ATSInfo, detect_ats
from strata_harvest.models import FetchResult, JobListing, ScrapeResult

__all__ = [
    "ATSInfo",
    "Crawler",
    "FetchResult",
    "JobListing",
    "ScrapeResult",
    "create_crawler",
    "detect_ats",
    "harvest",
]

__version__ = "0.1.0"
