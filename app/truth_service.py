
import os
from pprint import pprint
from datetime import timedelta, datetime, timezone
from dateutil import parser as date_parser

from truthbrush import Api
from dotenv import load_dotenv

from app.bq_service import generate_timestamp

load_dotenv()

TRUTH_USERNAME = os.getenv("TRUTH_USERNAME")
TRUTH_PASSWORD = os.getenv("TRUTH_PASSWORD")
COLLECTION_USERNAME= os.getenv("COLLECTION_USERNAME", default="realDonaldTrump")
VERBOSE_MODE = bool(os.getenv("VERBOSE_MODE") == "true")

class TruthService:
    def __init__(self, username=TRUTH_USERNAME, password=TRUTH_PASSWORD):
        self.client = Api(username=username, password=password)

    def get_user(self, username=COLLECTION_USERNAME):
        return self.client.lookup(user_handle=username)

    def get_user_timeline(self, username=COLLECTION_USERNAME, replies=True, verbose=VERBOSE_MODE, since_id=None, created_after=None):
        return self.client.pull_statuses(username=username, replies=replies, verbose=verbose, since_id=since_id, created_after=created_after)

    @staticmethod
    def parse_tag(tag):
        # tag["history"]
        #>[
        #>{'accounts': '1453', 'day': '1721606400', 'days_ago': 0, 'uses': '4272'},
        #>{'accounts': '860', 'day': '1721520000', 'days_ago': 1, 'uses': '2277'},
        #>{'accounts': '981', 'day': '1721433600', 'days_ago': 2, 'uses': '2548'},
        #>{'accounts': '1255', 'day': '1721347200', 'days_ago': 3, 'uses': '3373'},
        #>{'accounts': '1058', 'day': '1721260800', 'days_ago': 4, 'uses': '2995'},
        #>{'accounts': '1039', 'day': '1721174400', 'days_ago': 5, 'uses': '2978'},
        #>{'accounts': '1489', 'day': '1721088000', 'days_ago': 6, 'uses': '3907'}
        #>]
        # looks like they are in reverse chronological order
        users, uses, days = [], [], []

        for item in tag["history"]:
            users.append(item["accounts"])
            uses.append(item["uses"]) # could be posts. or maybe double counted if duplicated tag appears in single post?
            days.append(item["day"])

        return {
            "name": tag["name"],
            "recent_statuses_count": tag["recent_statuses_count"],
            #"recent_history": tag["recent_history"],
            "recent_users": users,
            "recent_uses": uses,
            "recent_days": days
        }

    @staticmethod
    def parse_status(status):
        # PARSER FUNCTIONS (FOR CONVERTING RAW DATA INTO DATABASE RECORDS):

        #breakpoint()

        # convert into timestamp format bigquery likes
        created_at = status["created_at"] #> # '2023-09-01T14:39:30.235Z'
        created_at = datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S.%fZ') #> datetime.datetime(2023, 9, 1, 14, 39, 30, 235000)
        created_at = generate_timestamp(created_at) #> '2023-09-01 14:39:30'

        try:
            group_id = status["group"]["id"]
            group_slug = status["group"]["slug"]
        except (KeyError, TypeError):
            # most don't have a group (only status is if posted to a group)
            group_id, group_slug = None, None

        try:
            reply_status_id = status["in_reply_to"]["id"]
            reply_user_id = status["in_reply_to_account_id"]
        except (KeyError, TypeError):
            # most are not replies
            reply_status_id, reply_user_id = None, None

        try:
            # ~78% have 0, ~21% have 1, <1% have more
            # ... so we are only storing at most one media attachment
            media_id = status["media_attachments"][0]["id"]
            media_type = status["media_attachments"][0]["type"]
            media_url = status["media_attachments"][0]["url"]
        except (KeyError, TypeError, IndexError):
            media_type, media_url, media_id = None, None, None

        try:
            # 92% have no mentions, 8% have one, <1% have multiple
            # ... so we are only storing at most one mention
            mention_id = status["mentions"][0]["id"]
            mention_username = status["mentions"][0]["username"]
            # FYI: the id of the mentioned user is not available
        except (KeyError, TypeError, IndexError):
            mention_id, mention_username = None, None

        if any(status["tags"]):
            # 99% have no tags, <1% have tags
            # ... they look like this: {'name': 'Agenda47'}
            tags = [t["name"] for t in status["tags"]]
        else:
            tags = None

        return {
            "status_id": status["id"],
            "user_id": status["account"]["id"],
            "username": status["account"]["username"],
            "created_at": created_at,
            "lang": status["language"],
            "content": status["content"],

            "group_id": group_id,
            "group_slug": group_slug,

            "reply_status_id": reply_status_id,
            "reply_user_id": reply_user_id,

            "media_id": media_id,
            "media_type": media_type,
            "media_url": media_url,

            "mention_id": mention_id,
            "mention_username": mention_username,

            "tags": tags

        }




def to_utc(date_str):
    """Datetime formatter function. Ensures timezone is UTC."""
    if not isinstance(date_str, str):
        date_str = str(date_str) # handle datetime objects
    return date_parser.parse(date_str).replace(tzinfo=timezone.utc)







if __name__ == "__main__":

    from pandas import DataFrame
    from app.exporters import EXPORTS_DIR, Database

    db = Database()

    service = TruthService()

    tags = service.client.tags()
    records = [service.parse_tag(tag) for tag in tags]
    print(len(records))
    df = DataFrame(records)
    print(df.head())

    csv_filepath = os.path.join(EXPORTS_DIR, "trending_tags.csv")
    print("EXPORT TO CSV:", csv_filepath)
    df.to_csv(csv_filepath, index=False)

    print("EXPORT TO SQLITE:")
    db.insert_df(df=df, table_name="trending_tags")


    #print("----------")
    #print("USER:")
    #user = service.get_user()
    #pprint(user)





    print("----------")
    print("USER TIMELINE...")

    recent = datetime.now() - timedelta(days=7)
    recent_tz = to_utc(recent)
    verbose = True
    timeline = list(service.get_user_timeline(created_after=recent_tz, verbose=verbose))
    print(len(timeline))

    records = [service.parse_status(status) for status in timeline]
    print(len(records))
    df = DataFrame(records)
    print(df.head())

    csv_filepath = os.path.join(EXPORTS_DIR, "user_timeline.csv")
    print("EXPORT TO CSV:", csv_filepath)
    df.to_csv(csv_filepath, index=False)

    print("EXPORT TO SQLITE:")
    db.insert_df(df=df, table_name="user_timeline")
