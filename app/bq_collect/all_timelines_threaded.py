
import os
from concurrent.futures import ThreadPoolExecutor #, as_completed

from app import server_sleep

from app.bq_collect.timeline_statuses import update_timeline_statuses
from app.bq_collect.all_timelines import AllTimelinesJob

USERS_LIMIT = os.getenv("USERS_LIMIT")
MAX_THREADS = int(os.getenv("MAX_THREADS", default="3"))


if __name__ == "__main__":

    job = AllTimelinesJob()
    users_df = job.users_df
    print("USERS:", len(users_df))

    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        for i, row in users_df.iterrows():
            username = row["username"]
            since_id = row["latest_status_id"]
            executor.submit(update_timeline_statuses, username=username, bq=job.bq, truth=job.truth, verbose=False, since_id=since_id)

    server_sleep()
