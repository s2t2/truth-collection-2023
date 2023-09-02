
from app import server_sleep
from app.bq_service import BigQueryService, generate_timestamp
from app.truth_service import COLLECTION_USERNAME, TruthService, parse_status


if __name__ == "__main__":

    bq = BigQueryService()
    truth = TruthService()
    username = COLLECTION_USERNAME

    print("--------------------")
    print("PREVIOUS RESULTS?")
    # what is the latest status we have for this person?
    sql = f"""
        SELECT status_id, created_at
        FROM {bq.dataset_address}.timeline_statuses
        WHERE UPPER(username) = '{username.upper()}'
        ORDER BY created_at DESC
        LIMIT 1
    """
    results = list(bq.execute_query(sql))
    print(results)

    print("--------------------")
    print("FETCH STATUSES...")
    since_id = results[0]["status_id"] if any(results) else None
    print("SINCE:", since_id)
    timeline = truth.get_user_timeline(username=username, since_id=since_id)

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

    server_sleep()
