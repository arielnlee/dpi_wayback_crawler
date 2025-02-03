from dateutil.relativedelta import relativedelta


class WaybackConstants:
    """Constants for Wayback Machine API interaction"""

    BASE_URL: str = "https://web.archive.org"
    CDX_BASE_URL: str = f"{BASE_URL}/cdx/search/cdx"

    # Frequency mapping for snapshot collection
    FREQUENCY_MAP = {
        "daily": ("timestamp:8", "%Y-%m-%d", relativedelta(days=1)),
        "monthly": ("timestamp:6", "%Y-%m", relativedelta(months=1)),
        "annually": ("timestamp:4", "%Y", relativedelta(years=1)),
    }


class CDXEndpoint:
    """
    Configuration for CDX API endpoints and parameters

    Filters
        -------
        - '&from={start_date}&to={end_date}': Specific date range.
        - '&filter=!statuscode:404': Exclude snapshots with status code 404.
        - '&filter=!mimetype:warc/revisit': Exclude snapshots with MIME type "warc/revisit" (revisit record without content).
        - '&collapse={collapse_filter}': Collapse feature to group snapshots based on desired frequency.
        - '&fl=timestamp,original,mimetype,statuscode,digest': Field list to include in the API response.
    """

    # Required fields for CDX API response
    FIELDS: list[str] = [
        "timestamp",  # When the snapshot was taken
        "original",  # Original URL
        "mimetype",  # Content type
        "statuscode",  # HTTP status
        "digest",  # Content hash
    ]
    # Standard exclusion filters
    FILTERS: list[str] = [
        "!statuscode:404",  # Exclude not found pages
        "!mimetype:warc/revisit",  # Exclude duplicate content
    ]
    # Rate limiting settings
    RATE_LIMIT_CALLS: int = 2
    RATE_LIMIT_PERIOD: int = 1  # seconds

    # Default user agent for robots.txt requests
    DEFAULT_USER_AGENT: str = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
