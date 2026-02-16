import pandas as pd
from rapidfuzz import process, fuzz
from sqlalchemy import text
from src.db.connection import PostgresConnector
from src.matching.ai_validator import AIValidator
import json


class EntityMatcher:
    def __init__(self, db_password, threshold=85):
        self.db = PostgresConnector(password=db_password)
        self.threshold = threshold

    # ---------------------------------
    # Load Clean Datasets
    # ---------------------------------
    def load_data(self):
        print("Loading clean datasets...")

        cc_query = """
        SELECT website_url, company_name, normalized_name
        FROM staging.commoncrawl_clean
        """

        abr_query = """
        SELECT abn, entity_name, normalized_name,
               entity_type, entity_status,
               state, postcode
        FROM staging.abr_clean
        """

        cc_df = pd.read_sql(cc_query, self.db.engine)
        abr_df = pd.read_sql(abr_query, self.db.engine)

        print(f"Common Crawl records: {len(cc_df)}")
        print(f"ABR records: {len(abr_df)}")

        return cc_df, abr_df

    # ---------------------------------
    # Perform Fuzzy + AI Matching (Optimized with Blocking)
    # ---------------------------------
    def fuzzy_match(self, cc_df, abr_df):
        print("Starting fuzzy matching with blocking...")

        ai_validator = AIValidator()
        matches = []

        abr_df = abr_df.dropna(subset=["normalized_name"])
        abr_df["first_char"] = abr_df["normalized_name"].str[0]

        abr_groups = {
            char: group.reset_index(drop=True)
            for char, group in abr_df.groupby("first_char")
        }

        total_processed = 0
        total_ai_calls = 0
        total_auto = 0

        for _, cc_row in cc_df.iterrows():
            total_processed += 1
            cc_name = cc_row["normalized_name"]

            if not cc_name:
                continue

            first_char = cc_name[0]

            if first_char not in abr_groups:
                continue

            candidate_group = abr_groups[first_char]
            abr_names = candidate_group["normalized_name"].tolist()

            match = process.extractOne(
                cc_name,
                abr_names,
                scorer=fuzz.token_sort_ratio
            )

            if not match:
                continue

            best_match_name, score, index = match
            abr_row = candidate_group.iloc[index]

            # 🥇 Auto accept
            if score >= self.threshold:
                total_auto += 1

                matches.append({
                    "abn": abr_row["abn"],
                    "website_url": cc_row["website_url"],
                    "company_name": abr_row["entity_name"],
                    "industry": None,
                    "entity_type": abr_row["entity_type"],
                    "entity_status": abr_row["entity_status"],
                    "state": abr_row["state"],
                    "postcode": abr_row["postcode"],
                    "match_method": "fuzzy_auto",
                    "match_confidence": float(score)
                })

            # 🥈 AI validation
            elif 75 <= score < self.threshold:
                total_ai_calls += 1
                print(f"[AI Triggered] {cc_row['company_name']} ↔ {abr_row['entity_name']} | Score: {score}")

                ai_result = ai_validator.validate(
                    cc_row["company_name"],
                    abr_row["entity_name"]
                )

                parsed = ai_result["parsed"]

                with self.db.engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO core.ai_match_log
                            (company_a, company_b, fuzzy_score, prompt, llm_response, decision)
                            VALUES (:a, :b, :score, :prompt, :response, :decision)
                        """),
                        {
                            "a": cc_row["company_name"],
                            "b": abr_row["entity_name"],
                            "score": float(score),
                            "prompt": ai_result["prompt"],
                            "response": json.dumps(parsed),
                            "decision": parsed["same_entity"]
                        }
                    )

                if parsed["same_entity"]:
                    matches.append({
                        "abn": abr_row["abn"],
                        "website_url": cc_row["website_url"],
                        "company_name": abr_row["entity_name"],
                        "industry": None,
                        "entity_type": abr_row["entity_type"],
                        "entity_status": abr_row["entity_status"],
                        "state": abr_row["state"],
                        "postcode": abr_row["postcode"],
                        "match_method": "ai_validated",
                        "match_confidence": float(parsed["confidence"] * 100)
                    })

            # Print progress every 50 records
            if total_processed % 50 == 0:
                print(f"Processed: {total_processed} | Auto: {total_auto} | AI Calls: {total_ai_calls}")

        print(f"Total processed: {total_processed}")
        print(f"Total fuzzy_auto: {total_auto}")
        print(f"Total AI calls: {total_ai_calls}")
        print(f"Total matches found: {len(matches)}")

        return pd.DataFrame(matches)



    # ---------------------------------
    # Insert Into core.company_master
    # ---------------------------------
    def insert_matches(self, matches_df):
        if matches_df.empty:
            print("No matches to insert.")
            return

        with self.db.engine.begin() as conn:
            conn.execute(text("DELETE FROM core.company_master"))

        matches_df.to_sql(
            name="company_master",
            con=self.db.engine,
            schema="core",
            if_exists="append",
            index=False,
            method="multi",
        )

        print("Inserted matches into core.company_master")

    # ---------------------------------
    # Run Full Matching Pipeline
    # ---------------------------------
    def run(self):
        cc_df, abr_df = self.load_data()
        matches_df = self.fuzzy_match(cc_df, abr_df)
        self.insert_matches(matches_df)
        print("Matching process completed.")


# ---------------------------------
# Run Script
# ---------------------------------
if __name__ == "__main__":
    PASSWORD = "firmable"

    matcher = EntityMatcher(
        db_password=PASSWORD,
        threshold=85
    )

    matcher.run()
