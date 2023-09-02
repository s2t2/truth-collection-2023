
import os
from dotenv import load_dotenv

from app.bq_service import BigQueryService

load_dotenv()

DESTRUCTIVE = bool(os.getenv("DESTRUCTIVE", default="false") == "true")

if __name__ == "__main__":

    bq = BigQueryService()

    bq.migrate_timeline_statuses_table(destructive=DESTRUCTIVE)

    print("MIGRATION OK")
