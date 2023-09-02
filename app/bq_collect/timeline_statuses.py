
#import os
#from dotenv import load_dotenv
from datetime import datetime

from app.bq_service import BigQueryService, generate_timestamp
from app.truth_service import BEGINNING_OF_TIME, COLLECTION_USERNAME, TruthService, parse_status


if __name__ == "__main__":

    bq = BigQueryService()
    truth = TruthService()
    username = COLLECTION_USERNAME

    print("--------------------")
    print("PREVIOUS RESULTS?")
    # what is the latest status we have for this person?
    sql = f"""
        SELECT max(created_at) as last_status_created_at
        FROM {bq.dataset_address}.timeline_statuses
        WHERE UPPER(username) = '{username.upper()}'
    """
    results = list(bq.execute_query(sql))
    latest = results[0]["last_status_created_at"] #> None or datetime.datetime(2023, 9, 1, 18, 9, 5, tzinfo=datetime.timezone.utc)
    # created_after = latest.date() if latest else BEGINNING_OF_TIME
    created_after = latest or BEGINNING_OF_TIME

    print("--------------------")
    print("FETCH STATUSES...")
    print("SINCE:", created_after)
    timeline = truth.get_user_timeline(username=username, created_after=created_after)

    print("--------------------")
    print("PARSE STATUSES...")
    records = []
    collected_at = generate_timestamp() # using the same collection time for each run may help identify collection runs later
    for status in timeline:
        record = parse_status(status)
        record["collected_at"] = collected_at # consider adding this to the parse_status method
        records.append(record)
    print("COLLECTED:", len(records))

    print("--------------------")
    print("SAVE STATUSES...")
    table = bq.timeline_statuses_table # api call table reference. after initial migration table ref might not yet be available, so we cound consider check here first before going through the trouble of fetching the timeline
    errors = bq.insert_records_in_batches(table, records)
    if any(errors):
        print("ERRORS:")
        print(errors)
