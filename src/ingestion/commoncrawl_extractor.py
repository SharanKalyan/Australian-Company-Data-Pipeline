import requests
import json
import re
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
from io import BytesIO
from bs4 import BeautifulSoup
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import os
from requests.exceptions import ChunkedEncodingError, ConnectionError
import time
from src.db.connection import PostgresConnector


class CommonCrawlExtractor:
    def __init__(self, crawl_index="CC-MAIN-2025-13", limit=5000):
        self.crawl_index = crawl_index
        self.limit = limit
        self.base_url = f"https://index.commoncrawl.org/{self.crawl_index}-index"

    # ---------------------------
    # URL Filtering
    # ---------------------------
    def is_valid_company_url(self, url):
        if not url:
            return False

        parsed = urlparse(url)
        domain = parsed.hostname.lower() if parsed.hostname else ""

        # Must be Australian domain
        if not domain.endswith(".au"):
            return False

        # Exclude robots
        if "robots.txt" in url:
            return False

        # Exclude obvious IP addresses
        if re.search(r"\d+\.\d+\.\d+\.\d+", domain):
            return False

        # Exclude too many digits (likely spam)
        if sum(c.isdigit() for c in domain) > 4:
            return False

        # Exclude weird subdomains like ww38
        if domain.startswith("ww") and not domain.startswith("www"):
            return False

        return True

    # ---------------------------
    # Fetch Index Records (Paginated + Deduped)
    # ---------------------------
    def fetch_index_records(self):
        base_params = {
            "url": "*.au",
            "output": "json"
        }

        count = 0
        page = 0
        max_retries = 3
        seen_domains = set()

        while count < self.limit:
            params = base_params.copy()
            params["page"] = page

            retries = 0

            while retries < max_retries:
                try:
                    print(f"Fetching index page {page} (attempt {retries+1})...")

                    response = requests.get(
                        self.base_url,
                        params=params,
                        stream=True,
                        timeout=60
                    )

                    if response.status_code != 200:
                        print("No more pages available.")
                        return

                    lines_found = False

                    for line in response.iter_lines():
                        if count >= self.limit:
                            return

                        if not line:
                            continue

                        lines_found = True
                        record = json.loads(line)
                        url = record.get("url")

                        if not self.is_valid_company_url(url):
                            continue

                        parsed = urlparse(url)
                        domain = parsed.hostname.lower() if parsed.hostname else ""

                        # Deduplicate by domain
                        if domain in seen_domains:
                            continue

                        seen_domains.add(domain)

                        yield record
                        count += 1

                    if not lines_found:
                        print("No results in this page.")
                        return

                    print(f"Page {page} complete. Unique domains so far: {count}")
                    break  # success

                except (ChunkedEncodingError, ConnectionError):
                    retries += 1
                    print(f"Connection dropped. Retrying page {page}...")
                    time.sleep(2)

            if retries == max_retries:
                print(f"Skipping page {page} after retries.")

            page += 1

    # ---------------------------
    # Download WARC Segment
    # ---------------------------
    def download_warc_segment(self, record):
        filename = record["filename"]
        offset = int(record["offset"])
        length = int(record["length"])

        warc_url = f"https://data.commoncrawl.org/{filename}"

        headers = {
            "Range": f"bytes={offset}-{offset+length-1}"
        }

        try:
            response = requests.get(warc_url, headers=headers, timeout=30)

            if response.status_code != 206:
                return None

            return response.content

        except Exception:
            return None

    # ---------------------------
    # Extract Company Info
    # ---------------------------
    def extract_company_info(self, html_bytes, url):
        try:
            stream = BytesIO(html_bytes)

            for warc_record in ArchiveIterator(stream):
                if warc_record.rec_type == "response":
                    html = warc_record.content_stream().read()
                    soup = BeautifulSoup(html, "html.parser")

                    company_name = None

                    # Priority extraction order
                    og_site = soup.find("meta", property="og:site_name")
                    if og_site and og_site.get("content"):
                        company_name = og_site["content"].strip()

                    if not company_name:
                        og_title = soup.find("meta", property="og:title")
                        if og_title and og_title.get("content"):
                            company_name = og_title["content"].strip()

                    if not company_name:
                        app_name = soup.find("meta", attrs={"name": "application-name"})
                        if app_name and app_name.get("content"):
                            company_name = app_name["content"].strip()

                    if not company_name:
                        twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
                        if twitter_title and twitter_title.get("content"):
                            company_name = twitter_title["content"].strip()

                    if not company_name:
                        h1 = soup.find("h1")
                        if h1:
                            company_name = h1.get_text().strip()

                    if not company_name and soup.title:
                        company_name = soup.title.string.strip()

                    # Junk filter
                    if company_name:
                        junk_words = ["home", "welcome", "index", "untitled", "default"]
                        if company_name.lower() in junk_words:
                            return None

                    # Industry extraction
                    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
                    keywords = meta_keywords["content"] if meta_keywords and "content" in meta_keywords.attrs else None

                    meta_desc = soup.find("meta", attrs={"name": "description"})
                    description = meta_desc["content"] if meta_desc and "content" in meta_desc.attrs else None

                    industry = keywords if keywords else description

                    if company_name:
                        return {
                            "website_url": url,
                            "company_name": company_name,
                            "industry": industry
                        }

        except Exception:
            return None

        return None

    # ---------------------------
    # Run Full Extraction
    # ---------------------------
    def run_extraction(self):
        results = []

        print(f"Starting extraction for up to {self.limit} unique domains...")

        for record in tqdm(self.fetch_index_records()):
            try:
                warc_data = self.download_warc_segment(record)

                if not warc_data:
                    continue

                company_data = self.extract_company_info(
                    warc_data, record["url"]
                )

                if company_data:
                    results.append(company_data)

            except Exception:
                continue

        df = pd.DataFrame(results)
        print(f"\nExtraction complete. Extracted {len(df)} unique company records.")

        return df

    # ---------------------------
    # Save Raw JSON
    # ---------------------------
    def save_raw_output(self, df):
        os.makedirs("data/raw/commoncrawl", exist_ok=True)

        filename = f"data/raw/commoncrawl/commoncrawl_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        df.to_json(filename, orient="records", indent=2)

        print(f"Raw data saved to {filename}")

    # ---------------------------
    # Insert Into Postgres
    # ---------------------------
    def insert_into_db(self, df, db_password):
        db = PostgresConnector(password=db_password)

        db.insert_dataframe(
            df=df,
            table_name="commoncrawl_raw",
            schema="staging"
        )

        print("Data inserted into staging.commoncrawl_raw")


# ---------------------------
# Run Script
# ---------------------------
if __name__ == "__main__":
    PASSWORD = "firmable"

    extractor = CommonCrawlExtractor(limit=1000)
    df = extractor.run_extraction()

    if not df.empty:
        extractor.save_raw_output(df)
        extractor.insert_into_db(df, db_password=PASSWORD)
        print(df.head())
    else:
        print("No valid records extracted.")
