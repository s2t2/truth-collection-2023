from imports.groups import GROUPS

from app.bq_service import BigQueryService
from app.truth_service import TruthService


def fetch_groups(bq):
    sql = f"""
        SELECT DISTINCT slug, display_name
        FROM {bq.dataset_address}.groups
        ORDER BY slug
    """
    return list(bq.execute_query(sql, verbose=False))


if __name__ == "__main__":

    ts = TruthService()
    bq = BigQueryService()

    groups_existing = fetch_groups(bq)
    slugs_existing = [group["slug"].upper() for group in groups_existing]

    groups = [group for group in GROUPS if group["slug"].upper() not in slugs_existing]
    records = []
    for group in groups:
        print("-------------")
        print(group)
        records.append({
            "slug": group["slug"],
            "display_name": group["display_name"]
        })

    if any(records):
        print("PROCESSED", len(records), "RECORDS")
        print("SAVING TO BQ...")
        bq.insert_records_in_batches(bq.groups_table, records)
