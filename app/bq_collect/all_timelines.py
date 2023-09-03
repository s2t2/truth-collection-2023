
import os

from app import server_sleep
from app.bq_service import BigQueryService
from app.truth_service import TruthService

from app.bq_collect.timeline_statuses import update_timeline_statuses

USERS_LIMIT = os.getenv("USERS_LIMIT")


if __name__ == "__main__":

    bq = BigQueryService()
    truth = TruthService()
    users_limit = USERS_LIMIT

    print("--------------------")
    print("COLLECTED USERS...")
    # which users timelines have we previously collected
    collected_df = bq.query_to_df(f"""
        SELECT DISTINCT UPPER(username) as username
        FROM {bq.dataset_address}.timeline_statuses
    """)
    collected_usernames = collected_df["username"].tolist()
    print(len(collected_usernames))

    print("--------------------")
    print("MENTIONED USERS...")
    # which users have been mentioned by any previously collected
    sql = f"""
        SELECT UPPER(mention_username) as mention_username
            ,count(DISTINCT status_id) as status_count
        FROM {bq.dataset_address}.timeline_statuses
        WHERE mention_username IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
    """
    if users_limit:
        sql += f" LIMIT {int(users_limit)}"
    mentioned_df = bq.query_to_df(sql).sort_values(by="status_count", ascending=False) # apparently sort order not coming through to df?
    mentioned_usernames = mentioned_df["mention_username"].tolist()
    print(len(mentioned_usernames))
    print(mentioned_df.head())

    print("--------------------")
    print("USERS FOR COLLECTION...")
    usernames = sorted(list(set(mentioned_usernames + collected_usernames)))
    print(len(usernames))

    for i, username in enumerate(usernames):
        print("--------------------")
        print("USERNAME:", i, username)
        update_timeline_statuses(username=username, bq=bq, truth=truth, verbose=False)

    server_sleep()
