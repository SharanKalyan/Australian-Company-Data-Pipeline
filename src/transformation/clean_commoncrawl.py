import re
import pandas as pd
from sqlalchemy import text
from src.db.connection import PostgresConnector


class CommonCrawlCleaner:
    def __init__(self, db_password):
        self.db = PostgresConnector(password=db_password)

    # ---------------------------
    # Normalize Company Name
    # ---------------------------
    def normalize_name(self, name):
        if not name:
            return None

        name = name.lower()

        # Remove common business suffixes
        name = re.sub(r"\bpty ltd\b", "", name)
        name = re.sub(r"\bltd\b", "", name)
        name = re.sub(r"\blimited\b", "", name)
        name = re.sub(r"\bpty\b", "", name)

        # Remove marketing phrases
        name = re.sub(r"\bintroducing\b", "", name)
        name = re.sub(r"\bofficial website\b", "", name)
        name = re.sub(r"\bhome\b", "", name)
        name = re.sub(r"\bwelcome\b", "", name)

        # Remove punctuation
        name = re.sub(r"[^\w\s]", "", name)

        # Remove extra spaces
        name = re.sub(r"\s+", " ", name)

        return name.strip()

    # ---------------------------
    # Clean Raw Data
    # ---------------------------
    def clean_dataframe(self, df):

        # Remove null or empty names
        df = df[df["company_name"].notnull()]
        df = df[df["company_name"].str.strip() != ""]

        # Remove numeric-only names
        df = df[~df["company_name"].str.match(r"^\d+$")]

        # Remove names that look like addresses (start with number + street)
        df = df[~df["company_name"].str.match(r"^\d+\s+\w+", na=False)]

        # Remove domain-looking titles
        df = df[~df["company_name"].str.contains(r"\.au$", case=False, na=False)]

        # Remove very short names
        df = df[df["company_name"].str.len() >= 4]

        # Remove names that start with number
        df = df[~df["company_name"].str.match(r"^\d", na=False)]

        # Handle titles like "Name - Something"
        df["company_name"] = df["company_name"].apply(
            lambda x: x.split(" - ")[0] if " - " in x else x
        )

        # Standardize casing
        df["company_name"] = df["company_name"].str.strip().str.title()

        # Create normalized_name
        df["normalized_name"] = df["company_name"].apply(self.normalize_name)

        # Remove rows where normalized name becomes empty
        df = df[df["normalized_name"].notnull()]
        df = df[df["normalized_name"].str.strip() != ""]

        # Drop duplicates
        df = df.drop_duplicates(subset=["normalized_name"])

        return df

    # ---------------------------
    # Run Cleaning Pipeline
    # ---------------------------
    def run(self):
        query = """
        SELECT website_url, company_name, industry
        FROM staging.commoncrawl_raw
        """

        df = pd.read_sql(query, self.db.engine)

        print(f"Loaded {len(df)} raw records")

        df_clean = self.clean_dataframe(df)

        print(f"{len(df_clean)} records remaining after cleaning")

        # Clear existing clean table before insert
        with self.db.engine.begin() as conn:
            conn.execute(text("DELETE FROM staging.commoncrawl_clean"))

        df_clean.to_sql(
            name="commoncrawl_clean",
            con=self.db.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )

        print("Data inserted into staging.commoncrawl_clean")


# ---------------------------
# Run Script
# ---------------------------
if __name__ == "__main__":
    PASSWORD = "firmable"

    cleaner = CommonCrawlCleaner(db_password=PASSWORD)
    cleaner.run()
