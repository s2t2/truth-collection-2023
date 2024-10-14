
import os
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DESTRUCTIVE = bool(os.getenv("DESTRUCTIVE", default="false") == "true")


def migrate_group_profiles_table(bq, destructive=False):
    """WARNING: DESTRUCTIVE MODE WILL DELETE THE TABLE!!!"""
    sql = ""
    if destructive:
        sql += f"DROP TABLE IF EXISTS {bq.dataset_address}.group_profiles; "

    sql += f"""
        CREATE TABLE IF NOT EXISTS {bq.dataset_address}.group_profiles(
            -- collect identifiers as strings for now, will convert to int later if possible for faster joins

            group_id STRING, -- v2: convert to INT64
            owner_id STRING, -- v2: convert to INT64
            slug STRING,
            created_at TIMESTAMP,

            membership_required BOOL,
            visibility STRING,
            display_name STRING,
            members_count INT64,
            avatar_url STRING,
            header_url STRING,
            note_html STRING, -- HTML text, for v2: also make a column of just the text, if possible
            tag_names ARRAY<STRING>,

            collected_at TIMESTAMP -- FYI some metadata from us
        );
    """
    bq.execute_query(sql)


if __name__ == "__main__":

    bq = BigQueryService()

    migrate_group_profiles_table(bq, destructive=DESTRUCTIVE)

    print("MIGRATION OK")
