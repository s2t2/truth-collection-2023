
import os
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DESTRUCTIVE = bool(os.getenv("DESTRUCTIVE", default="false") == "true")

def migrate_trending_tags_table(bq, destructive=False):
    """WARNING: DESTRUCTIVE MODE WILL DELETE THE TABLE!!!"""
    sql = ""
    if destructive:
        sql += f"DROP TABLE IF EXISTS {bq.dataset_address}.trending_tags; "

    sql += f"""
        CREATE TABLE IF NOT EXISTS {bq.dataset_address}.trending_tags(

            name                    STRING,
            recent_status_count     INT64,
            -- recent_history       ARRAY<STRING>,
            recent_users            ARRAY<INT64>,
            recent_uses             ARRAY<INT64>,
            recent_days             ARRAY<INT64>,
            -- todo v2: calculate mean and median

            collected_at            TIMESTAMP -- some metadata from us

        );
    """
    bq.execute_query(sql)


if __name__ == "__main__":

    bq = BigQueryService()

    migrate_trending_tags_table(bq, destructive=DESTRUCTIVE)

    print("MIGRATION OK")
