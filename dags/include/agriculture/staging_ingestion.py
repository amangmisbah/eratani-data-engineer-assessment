import logging
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

TABLE_NAME = "staging_agriculture"


def ingest_csv_to_staging(
    csv_path: str,
    postgres_conn_id: str
) -> int:
    """
    Ingest CSV data into PostgreSQL staging table.
    Idempotent by truncating table before insert.
    """

    logger.info("Reading CSV from path: %s", csv_path)
    df = pd.read_csv(csv_path)

    row_count = len(df)
    logger.info("CSV loaded successfully. Row count: %s", row_count)

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        logger.info("Ensuring staging table exists")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS staging_agriculture (
                farm_id VARCHAR(50),
                crop_type VARCHAR(50),
                farm_area_acres NUMERIC(10, 2),
                irrigation_type VARCHAR(50),
                fertilizer_used_tons NUMERIC(10, 2),
                pesticide_used_kg NUMERIC(10, 2),
                yield_tons NUMERIC(10, 2),
                soil_type VARCHAR(50),
                season VARCHAR(50),
                water_usage_cubic_meters NUMERIC(15, 2),
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        logger.info("Truncating staging table for idempotency")
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME}")

        insert_query = """
            INSERT INTO staging_agriculture (
                farm_id,
                crop_type,
                farm_area_acres,
                irrigation_type,
                fertilizer_used_tons,
                pesticide_used_kg,
                yield_tons,
                soil_type,
                season,
                water_usage_cubic_meters
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        records = [
            (
                row["Farm_ID"],
                row["Crop_Type"],
                row["Farm_Area(acres)"],
                row["Irrigation_Type"],
                row["Fertilizer_Used(tons)"],
                row["Pesticide_Used(kg)"],
                row["Yield(tons)"],
                row["Soil_Type"],
                row["Season"],
                row["Water_Usage(cubic meters)"],
            )
            for _, row in df.iterrows()
        ]

        cursor.executemany(insert_query, records)
        conn.commit()

        cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        final_count = cursor.fetchone()[0]

        logger.info(
            "Staging ingestion completed successfully. Rows inserted: %s",
            final_count,
        )

        return final_count

    finally:
        cursor.close()
        conn.close()
