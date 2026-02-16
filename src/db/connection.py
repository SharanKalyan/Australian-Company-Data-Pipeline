from sqlalchemy import create_engine, text
import pandas as pd


class PostgresConnector:
    def __init__(
        self,
        host="localhost",
        port=5432,
        database="firmable_db",
        user="postgres",
        password="firmable",
    ):
        self.connection_string = (
            f"postgresql://{user}:{password}@{host}:{port}/{database}"
        )
        self.engine = create_engine(self.connection_string, future=True)

    def insert_dataframe(self, df: pd.DataFrame, table_name: str, schema: str):
        df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
        )

    def test_connection(self):
        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT current_database();"))
            return result.fetchone()
