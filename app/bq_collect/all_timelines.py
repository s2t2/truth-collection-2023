
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
    print("PRIORITIZING USERS FOR COLLECTION...")
    sql = f"""
        SELECT coalesce(collected.username, mentioned.username) as username
            ,status_count, latest_status_id
            ,mention_by_user_count ,mention_status_count

        FROM (
            SELECT UPPER(username) as username
                ,count(DISTINCT status_id) as status_count
                ,max(status_id) as latest_status_id
            FROM {bq.dataset_address}.timeline_statuses
            GROUP BY 1
            -- ORDER BY 2 DESC
        ) collected

        FULL OUTER JOIN (
            SELECT UPPER(mention_username) as username
                ,count(DISTINCT user_id) as mention_by_user_count
                ,count(DISTINCT status_id) as mention_status_count
            FROM {bq.dataset_address}.timeline_statuses
            WHERE mention_username IS NOT NULL
            GROUP BY 1
        ) mentioned on collected.username = mentioned.username

        ORDER BY 4 DESC, 2 DESC
    """
    if users_limit:
        sql += f" LIMIT {int(users_limit)}"

    users_df = bq.query_to_df(sql, verbose=False).sort_values(by=["mention_by_user_count", "status_count"], ascending=False) # apparently sort order not coming through to df?
    print("USERS:", len(users_df))
    for i, row in users_df.iterrows():
        username = row["username"]
        since_id = row["latest_status_id"]
        update_timeline_statuses(username=username, bq=bq, truth=truth, verbose=False, since_id=since_id)

    server_sleep()
