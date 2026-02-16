import os
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm
from src.db.connection import PostgresConnector


class ABRParser:
    def __init__(self, db_password, data_path="data/raw/abr", limit=10000, batch_size=1000):
        self.db = PostgresConnector(password=db_password)
        self.data_path = data_path
        self.limit = limit
        self.batch_size = batch_size

    # ---------------------------------
    # Validate DB Connection First
    # ---------------------------------
    def validate_connection(self):
        try:
            result = self.db.test_connection()
            print(f"Connected to database: {result[0]}")
        except Exception as e:
            print("Database connection failed. Stopping execution.")
            raise e

    # ---------------------------------
    # Extract Required Fields (No Namespace)
    # ---------------------------------
    def extract_record(self, element):
        try:
            abn = element.findtext("ABN")

            entity_name = element.findtext(
                "MainEntity/NonIndividualName/NonIndividualNameText"
            )

            entity_type = element.findtext(
                "EntityType/EntityTypeText"
            )

            entity_status = element.find("ABN").attrib.get("status") if element.find("ABN") is not None else None

            postcode = element.findtext(
                "MainEntity/BusinessAddress/AddressDetails/Postcode"
            )

            state = element.findtext(
                "MainEntity/BusinessAddress/AddressDetails/State"
            )

            start_date = element.find("ABN").attrib.get("ABNStatusFromDate") if element.find("ABN") is not None else None

            return {
                "abn": abn,
                "entity_name": entity_name,
                "entity_type": entity_type,
                "entity_status": entity_status,
                "address_line": None,
                "postcode": postcode,
                "state": state,
                "start_date": start_date,
            }

        except Exception:
            return None

    # ---------------------------------
    # Parse Single XML File (Streaming)
    # ---------------------------------
    def parse_file(self, file_path):
        print(f"Processing file: {file_path}")

        records = []
        total_processed = 0

        context = ET.iterparse(file_path, events=("end",))

        for event, elem in context:
            if elem.tag == "ABR":
                record = self.extract_record(elem)

                if record and record["abn"]:
                    records.append(record)
                    total_processed += 1

                if len(records) >= self.batch_size:
                    self.insert_batch(records)
                    records = []

                if total_processed >= self.limit:
                    break

                elem.clear()

        if records:
            self.insert_batch(records)

        print(f"Finished file: {file_path}")
        return total_processed

    # ---------------------------------
    # Insert Batch Into DB
    # ---------------------------------
    def insert_batch(self, records):
        df = pd.DataFrame(records)

        df.to_sql(
            name="abr_raw",
            con=self.db.engine,
            schema="staging",
            if_exists="append",
            index=False,
            method="multi",
        )

        print(f"Inserted batch of {len(df)} records")

    # ---------------------------------
    # Run Full Parsing
    # ---------------------------------
    def run(self):
        self.validate_connection()

        xml_files = glob.glob(os.path.join(self.data_path, "*.xml"))

        total_count = 0

        for file in xml_files:
            count = self.parse_file(file)
            total_count += count

            if total_count >= self.limit:
                break

        print(f"\nTotal ABR records processed: {total_count}")


# ---------------------------------
# Run Script
# ---------------------------------
if __name__ == "__main__":
    PASSWORD = "firmable"

    parser = ABRParser(
        db_password=PASSWORD,
        limit=500000,     
        batch_size=1000
    )

    parser.run()
