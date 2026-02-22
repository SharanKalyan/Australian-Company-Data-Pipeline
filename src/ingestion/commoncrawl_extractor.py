import requests
import json
from urllib.parse import urlparse
import pandas as pd
import time
from requests.exceptions import ChunkedEncodingError, ConnectionError
from src.db.connection import PostgresConnector


class FastCommonCrawlExtractor:

    def __init__(
        self,
        crawl_index="CC-MAIN-2025-13",
        start_page=0,
        max_pages=100,
        batch_size=5000
    ):
        self.crawl_index = crawl_index
        self.start_page = start_page
        self.max_pages = max_pages
        self.batch_size = batch_size
        self.base_url = f"https://index.commoncrawl.org/{self.crawl_index}-index"

    def is_valid_domain(self, domain):
        if not domain:
            return False
        return domain.lower().endswith(".au")

    def domain_to_company_name(self, domain):
        name = domain.lower()
        name = name.replace("www.", "")
        name = name.replace(".com.au", "")
        name = name.replace(".net.au", "")
        name = name.replace(".org.au", "")
        name = name.replace(".asn.au", "")
        name = name.replace(".au", "")
        name = name.replace("-", " ")
        name = name.strip()
        return name.title()

    def insert_batch(self, domains_batch, db_password):

        if not domains_batch:
            return 0

        db = PostgresConnector(password=db_password)

        data = []

        for domain in domains_batch:
            data.append({
                "website_url": f"http://{domain}",
                "company_name": self.domain_to_company_name(domain),
                "industry": None
            })

        df = pd.DataFrame(data)

        df.to_sql(
            name="commoncrawl_raw",
            con=db.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000
        )

        print(f"Inserted batch of {len(df)} records into DB")

        return len(df)

    def run(self, db_password):

        base_params = {
            "url": "*.au",
            "output": "json"
        }

        seen_domains = set()
        domains_batch = []

        total_collected = 0
        total_inserted = 0

        end_page = self.start_page + self.max_pages

        for page in range(self.start_page, end_page):

            params = base_params.copy()
            params["page"] = page

            print(f"\nFetching page {page}...")

            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    stream=True,
                    timeout=60
                )

                if response.status_code != 200:
                    print("No more pages available.")
                    break

                page_count = 0

                for line in response.iter_lines(decode_unicode=True):

                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        # Skip malformed / partial lines from Common Crawl
                        continue

                    url = record.get("url")
                    if not url:
                        continue

                    parsed = urlparse(url)
                    domain = parsed.hostname

                    if not self.is_valid_domain(domain):
                        continue

                    if domain in seen_domains:
                        continue

                    seen_domains.add(domain)
                    domains_batch.append(domain)

                    page_count += 1
                    total_collected += 1

                    if len(domains_batch) >= self.batch_size:
                        inserted = self.insert_batch(domains_batch, db_password)
                        total_inserted += inserted
                        domains_batch = []

                print(f"Page {page} collected: {page_count}")
                print(f"Total collected so far: {total_collected}")
                print(f"Total inserted so far: {total_inserted}")

                time.sleep(2)

            except (ChunkedEncodingError, ConnectionError):
                print("Connection dropped. Cooling down...")
                time.sleep(10)
                continue

        # Insert remaining
        if domains_batch:
            inserted = self.insert_batch(domains_batch, db_password)
            total_inserted += inserted

        print("\nExtraction window complete.")
        print(f"Final collected: {total_collected}")
        print(f"Final inserted: {total_inserted}")


if __name__ == "__main__":

    PASSWORD = "firmable"

    extractor = FastCommonCrawlExtractor(
        start_page=272,
        max_pages=50,
        batch_size=1000
    )

    extractor.run(db_password=PASSWORD)