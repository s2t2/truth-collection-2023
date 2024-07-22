
from app import server_sleep
from app.bq_service import BigQueryService, generate_timestamp
from app.truth_service import COLLECTION_USERNAME, TruthService, VERBOSE_MODE


def update_timeline_statuses(username=COLLECTION_USERNAME, bq=None, ts=None, verbose=VERBOSE_MODE, since_id=None): # user_id=None
    bq = bq or BigQueryService()
    ts = ts or TruthService()

    #if user_id:
    #    where = f"user_id = '{user_id}'"
    #else:
    #    where = f"UPPER(username) = '{username.upper()}'"

    if not since_id:
        # what is the latest status we have for this person?
        sql = f"""
            SELECT status_id, created_at
            FROM {bq.dataset_address}.timeline_statuses
            WHERE UPPER(username) = '{username.upper()}'
            ORDER BY created_at DESC
            LIMIT 1
        """
        results = list(bq.execute_query(sql, verbose=False))
        if any(results):
            since_id = results[0]["status_id"]
            #since = results[0]["created_at"]

    print("------------")
    print(f"FETCHING STATUSES FOR '{username}'", "SINCE:", since_id, "...")
    #params = {"since_id": since_id, "verbose": verbose}
    #if user_id:
    #    params["user_id"] = user_id
    #    print(f"FETCHING STATUSES FOR '{user_id}'", "SINCE:", since_id, "...")
    #else:
    #    params["username"] = username
    #    print(f"FETCHING STATUSES FOR '{username}'", "SINCE:", since_id, "...")
    #timeline = ts.get_user_timeline(**params)
    timeline = ts.get_user_timeline(username=username, since_id=since_id, verbose=verbose)

    records = []
    collected_at = generate_timestamp() # using the same collection time for each run may help identify collection runs later
    for status in timeline:
        record = ts.parse_status(status)
        record["collected_at"] = collected_at # consider adding this to the parse_status method
        records.append(record)
    print("FETCHED:", len(records), f"FOR '{username}'")

    if any(records):
        print("SAVING...")
        table = bq.timeline_statuses_table # api call table reference. after initial migration table ref might not yet be available, so we could consider check here first before going through the trouble of fetching the timeline
        errors = bq.insert_records_in_batches(table, records)
        if any(errors):
            print("ERRORS:", f"FOR '{username}'")
            print(errors)




if __name__ == "__main__":

    update_timeline_statuses()

    server_sleep()
