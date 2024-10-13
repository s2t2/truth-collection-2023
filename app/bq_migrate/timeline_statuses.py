
import os
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DESTRUCTIVE = bool(os.getenv("DESTRUCTIVE", default="false") == "true")


def migrate_timeline_statuses_table(bq, destructive=False):
    """WARNING: DESTRUCTIVE MODE WILL DELETE THE TABLE!!!"""
    sql = ""
    if destructive:
        sql += f"DROP TABLE IF EXISTS {bq.dataset_address}.timeline_statuses; "

    sql += f"""
        CREATE TABLE IF NOT EXISTS {bq.dataset_address}.timeline_statuses(
            -- collect identifiers as strings for now, will convert to int later if possible for faster joins

            status_id STRING, -- v2: convert to INT64
            user_id STRING, -- v2: convert to INT64
            username STRING,
            created_at TIMESTAMP,
            lang STRING,
            content STRING, -- HTML text, for v2: also make a column of just the text, if possible

            group_id STRING, -- v2: convert to INT64
            group_slug STRING,

            reply_status_id STRING, -- v2: convert to INT64
            reply_user_id STRING, -- v2: convert to INT64

            media_id STRING, -- v2: convert to INT64
            media_type STRING,
            media_url STRING,

            mention_id STRING, -- v2: convert to INT64
            mention_username STRING,

            tags ARRAY<STRING>,

            collected_at TIMESTAMP -- FYI some metadata from us

        );
    """
    bq.execute_query(sql)


if __name__ == "__main__":

    bq = BigQueryService()

    migrate_timeline_statuses_table(bq, destructive=DESTRUCTIVE)

    print("MIGRATION OK")
