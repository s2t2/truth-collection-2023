

from app.truth_service import TruthService, VERBOSE_MODE
from app.bq_service import BigQueryService, generate_timestamp


def timeline_records(timeline):
    records = []
    collected_at = generate_timestamp() # using the same collection time for each run may help identify collection runs later
    for status in timeline:
        record = ts.parse_status(status)
        record["collected_at"] = collected_at # consider adding this to the parse_status method
        records.append(record)
    return records


def update_group_timeline_statuses(group_name, bq=None, ts=None, verbose=VERBOSE_MODE, since_id=None): # user_id=None
    bq = bq or BigQueryService()
    ts = ts or TruthService()

    if not since_id:
        # what is the latest status we have for this group?
        sql = f"""
            SELECT status_id, created_at
            FROM {bq.dataset_address}.group_timeline_statuses
            WHERE UPPER(group_name) = '{group_name.upper()}'
            ORDER BY created_at DESC
            LIMIT 1
        """
        results = list(bq.execute_query(sql, verbose=False))
        if any(results):
            since_id = results[0]["status_id"]
            #since = results[0]["created_at"]

    print("------------")
    print(f"FETCHING STATUSES FOR '{group_name}'", "SINCE:", since_id, "...")

    timeline = ts.get_group_timeline(group_name=group_name, since_id=since_id, verbose=verbose)

    records = timeline_records(timeline)
    print("FETCHED:", len(records), f"FOR '{group_name}'")

    if any(records):
        print("SAVING...")
        table = bq.group_timeline_statuses_table # api call table reference. after initial migration table ref might not yet be available, so we could consider check here first before going through the trouble of fetching the timeline
        errors = bq.insert_records_in_batches(table, records)
        if any(errors):
            print("ERRORS:", f"FOR '{group_name}'")
            print(errors)




if __name__ == "__main__":

    update_group_timeline_statuses()

    # for a single user, can run via cron job / heroku scheduler
    # once per day,
    # without sleeping or restarting



if __name__ == "__main__":


    ts = TruthService()
