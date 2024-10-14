
import os
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DESTRUCTIVE = bool(os.getenv("DESTRUCTIVE", default="false") == "true")


def migrate_groups_table(bq, destructive=False):
    """WARNING: DESTRUCTIVE MODE WILL DELETE THE TABLE!!!"""
    sql = ""
    if destructive:
        sql += f"DROP TABLE IF EXISTS {bq.dataset_address}.groups; "

    sql += f"""
        CREATE TABLE IF NOT EXISTS {bq.dataset_address}.groups(
            slug STRING,
            display_name STRING,
        );
    """
    bq.execute_query(sql)


if __name__ == "__main__":

    bq = BigQueryService()

    migrate_groups_table(bq, destructive=DESTRUCTIVE)

    print("MIGRATION OK")
