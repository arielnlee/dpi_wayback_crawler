# Historical Wayback Crawler

This script collects historical versions of webpages and extracts their raw text/HTML content for analysis and optionally saves the data to a json. It uses the Wayback Machine and [CDX API](https://github.com/internetarchive/wayback/blob/master/wayback-cdx-server/README.md) to retrieve snapshots. It can also track the number of times a given webpage has changed its content within the specified time range (does not do track by default).

Cleaned up from [Data Provenance Initiatve](https://github.com/Data-Provenance-Initiative/Data-Provenance-Collection/tree/main/src/web_analysis/wayback_extraction).

**Install Requirements**

```
pip install -r requirements.txt
```

**Usage**

NOTE: Your CSV file should contain a **"URL" column** of urls to crawl. The default is to crawl robots.txt files. If you want to crawl the main page/domain, use `--site-type main` so the URL is not modified to include the `/robots.txt` path.

```
python -m wayback.run \
    --input-path <in-path> \
    --snapshots-path snapshots \
    --output-json-path wayback_data.json \
    --start-date 20240419 \
    --end-date 20250203 \
    --frequency monthly \
    --site-type robots \
    --save-snapshots \
    --process-to-json
```

**Arguments**

- `--input-path` (Path, required): Path to CSV file containing URLs (**must include "URL" column**).
- `--output-json-path` (Path, default: `./wayback_data.json`): Path to save the output JSON file with extracted text for all URLs.
- `--start-date` (str, default: `"20240419"`): Start date in YYYYMMDD format.
- `--end-date` (str, default: `"20250203"`): End date in YYYYMMDD format.
- `--frequency` (str, default: `"monthly"`, choices: `["daily", "monthly", "annually"]`): Frequency of collecting snapshots.
- `--num-workers` (int, default: `multiprocessing.cpu_count() - 1`): Number of worker threads.
- `--snapshots-path` (Path, default: `Path("snapshots")`): Path to the folder where snapshots will be saved.
- `--stats-path` (Path, default: `Path("stats")`): Path to the folder where rate of change stats will be saved.
- `--count-changes` (flag, default: `False`): Track rate of change by counting the number of unique changes for each site in the date range.
- `--process-to-json` (flag, default: `False`): Process the extracted snapshots and save them to a JSON file.
- `--save-snapshots` (flag, default: `False`): Whether to save and process snapshots from the Wayback Machine.
- `--site-type` (str, default: `"robots"`, choices: `["tos", "robots", "main"]`): Type of site to process (terms of service, robots.txt, or main page/domain).
- `--max-chunk-size` (int, default: `5000`): Chunk size (MB) for saving data to JSON file.

The only required argument is the input path to a CSV file with URLs.

**Rate Limiting**

To avoid overwhelming sites and respect rate limits, this script uses the `ratelimit` library to limit the number of requests to 2 requests per second.

If you need to adjust the rate limit, modify the `RATE_LIMIT_CALLS` and `RATE_LIMIT_PERIOD` of the `CDXEndpoint` class in the `config.py` file.

**Errors**

Any errors / failed requests are saved to a file called `failed_urls.txt` in the root directory of this repo.

**Output JSON Format**

When using `--process-to-json`, the script creates a JSON file with the following structure:

```json
{
    "domain.com": {
        "YYYY-MM-DD": "content for this date",
        "YYYY-MM-DD": "content for this date",
        ...
    },
    "another-domain.com": {
        "YYYY-MM-DD": "content for this date",
        "YYYY-MM-DD": "content for this date",
        ...
    }
}
```

Example output for robots.txt files:

```json
{
  "patents.google.com": {
    "2024-04-19": "User-agent: *\nDisallow: /*\nAllow: /$\nAllow: /advanced$\nAllow: /patent/\nAllow: /sitemap/",
    "2024-05-01": "User-agent: *\nDisallow: /*\nAllow: /$\nAllow: /advanced$\nAllow: /patent/\nAllow: /sitemap/",
    "2024-06-01": "User-agent: *\nDisallow: /*\nAllow: /$\nAllow: /advanced$\nAllow: /patent/\nAllow: /sitemap/"
  }
}
```

The JSON structure is:

- Top level: Dictionary of domains
- Second level: Dictionary of dates mapping to content
- Content: Raw text/HTML content for that snapshot
- Dates: In YYYY-MM-DD format
