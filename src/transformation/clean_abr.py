import re
import pandas as pd
from sqlalchemy import text
from src.db.connection import PostgresConnector


class ABRCleaner:
    def __init__(self, db_password):
        self.db = PostgresConnector(password=db_password)

    # ---------------------------------
    # Normalize Company Name
    # ---------------------------------
    def normalize_name(self, name):
        if not name:
            return None

        name = name.lower()

        name = re.sub(r"\bpty ltd\b", "", name)
        name = re.sub(r"\bltd\b", "", name)
        name = re.sub(r"\blimited\b", "", name)
        name = re.sub(r"\bpty\b", "", name)

        name = re.sub(r"[^\w\s]", "", name)
        name = re.sub(r"\s+", " ", name)

        return name.strip()

    # ---------------------------------
    # Run Cleaning
    # ---------------------------------
    def run(self):
        print("Loading ABR raw data...")

        query = """
        SELECT abn, entity_name, entity_type, entity_status,
               address_line, postcode, state, start_date
        FROM staging.abr_raw
        """

        df = pd.read_sql(query, self.db.engine)

        print(f"Loaded {len(df)} raw ABR records")

        # Remove null names
        df = df[df["entity_name"].notnull()]
        df = df[df["entity_name"].str.strip() != ""]

        # Keep only active entities
        df = df[df["entity_status"] == "ACT"]

        # Standardize casing
        df["entity_name"] = df["entity_name"].str.strip().str.title()

        # Create normalized name
        df["normalized_name"] = df["entity_name"].apply(self.normalize_name)

        # Deduplicate by ABN
        df = df.drop_duplicates(subset=["abn"])

        print(f"{len(df)} records remaining after cleaning")

        # Clear existing clean table (dev purpose)
        with self.db.engine.begin() as conn:
            conn.execute(text("DELETE FROM staging.abr_clean"))

        # Insert into clean table
        df.to_sql(
            name="abr_clean",
            con=self.db.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )

        print("Data inserted into staging.abr_clean")


# ---------------------------------
# Run Script
# ---------------------------------
if __name__ == "__main__":
    PASSWORD = "firmable"

    cleaner = ABRCleaner(db_password=PASSWORD)
    cleaner.run()
