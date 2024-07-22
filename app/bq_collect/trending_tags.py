
from app import server_sleep
from app.bq_service import BigQueryService, generate_timestamp
from app.truth_service import TruthService #, VERBOSE_MODE


def update_trending_topics(bq=None, ts=None): # verbose=VERBOSE_MODE
    bq = bq or BigQueryService()
    ts = ts or TruthService()

    print("------------")
    print(f"FETCHING TRENDING TAGS...")

    trending = ts.client.tags() # verbose=verbose

    records = []
    collected_at = generate_timestamp() # using the same collection time for each run may help identify collection runs later
    for tag in trending:
        record = ts.parse_trending_tag(tag)
        record["collected_at"] = collected_at
        records.append(record)
    print("FETCHED:", len(records), "TRENDING TAGS")

    if any(records):
        print("SAVING...")
        table = bq.trending_tags_table # api call table reference. after initial migration table ref might not yet be available, so we could consider check here first before going through the trouble of fetching the timeline
        errors = bq.insert_records_in_batches(table, records)
        if any(errors):
            print("ERRORS:")
            print(errors)




if __name__ == "__main__":

    update_trending_topics()

    server_sleep()
