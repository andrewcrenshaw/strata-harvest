"""strata-harvest: Career page scraping and ATS parsing library."""

from strata_harvest.crawler import Crawler, create_crawler, harvest
from strata_harvest.detector import detect_ats, detect_from_dom, detect_from_url
from strata_harvest.models import ATSInfo, ATSProvider, FetchResult, JobListing, ScrapeResult

__all__ = [
    "ATSInfo",
    "ATSProvider",
    "Crawler",
    "FetchResult",
    "JobListing",
    "ScrapeResult",
    "create_crawler",
    "detect_ats",
    "detect_from_dom",
    "detect_from_url",
    "harvest",
]

__version__ = "0.1.0"
