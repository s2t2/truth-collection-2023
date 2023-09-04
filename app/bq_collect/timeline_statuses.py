
from app import server_sleep
from app.bq_service import BigQueryService, generate_timestamp
from app.truth_service import COLLECTION_USERNAME, TruthService, parse_status


def update_timeline_statuses(username=COLLECTION_USERNAME, bq=None, truth=None, verbose=True, since_id=None):
    bq = bq or BigQueryService()
    truth = truth or TruthService()

    print("------------")
    print(f"FETCH STATUSES FOR '{username}'...")

    if not since_id:
        # what is the latest status we have for this person?
        sql = f"""
            SELECT status_id, created_at
            FROM {bq.dataset_address}.timeline_statuses
            WHERE UPPER(username) = '{username.upper()}'
            ORDER BY created_at DESC
            LIMIT 1
        """
        results = list(bq.execute_query(sql))
        if any(results):
            since_id = results[0]["status_id"]
            #since = results[0]["created_at"]

    print("SINCE:", since_id)
    timeline = truth.get_user_timeline(username=username, since_id=since_id, verbose=verbose)
    #timeline = sorted(timeline, key=lambda post: post["id"])
    records = []
    collected_at = generate_timestamp() # using the same collection time for each run may help identify collection runs later
    for status in timeline:
        record = parse_status(status)
        record["collected_at"] = collected_at # consider adding this to the parse_status method
        records.append(record)
    print("FETCHED:", len(records))

    print("SAVE STATUSES...")
    table = bq.timeline_statuses_table # api call table reference. after initial migration table ref might not yet be available, so we cound consider check here first before going through the trouble of fetching the timeline
    errors = bq.insert_records_in_batches(table, records)
    if any(errors):
        print("ERRORS:")
        print(errors)




if __name__ == "__main__":

    update_timeline_statuses()

    server_sleep()
