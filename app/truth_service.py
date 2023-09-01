
import os
from pprint import pprint
from datetime import date, timedelta

from truthbrush import Api
from dotenv import load_dotenv

load_dotenv()

TRUTH_USERNAME = os.getenv("TRUTH_USERNAME")
TRUTH_PASSWORD = os.getenv("TRUTH_PASSWORD")

BEGINNING_OF_TIME = date(2022, 1, 1) # start of timeline



def parse_status(status):

    try:
        group_id = status["group"]["id"]
        group_slug = status["group"]["slug"]
    except (KeyError, TypeError):
        # most don't have a group (only status is if posted to a group)
        group_id, group_slug = None, None

    try:
        reply_status_id = status["in_reply_to"]["id"]
        reply_account_id = status["in_reply_to_account_id"]
    except (KeyError, TypeError):
        # most are not replies
        reply_status_id, reply_account_id = None, None

    try:
        # ~78% have 0, ~21% have 1, <1% have more
        # ... so we are only storing at most one media attachment
        media_type = status["media_attachments"][0]["type"]
        media_url = status["media_attachments"][0]["url"]
    except (KeyError, TypeError):
        media_type, media_url = None, None

    try:
        # 92% have no mentions, 8% have one, <1% have multiple
        # ... so we are only storing at most one mention
        mention_id = status["mentions"][0]["id"]
        mention_username = status["mentions"][0]["username"]
    except (KeyError, TypeError):
        mention_id, mention_username = None, None

    return {
        "status_id": status["id"],
        "account_id": status["account"]["id"],
        "created_at": status["created_at"],
        "lang": status["language"],
        "content": status["content"],

        "group_id": group_id,
        "group_slug": group_slug,

        "reply_status_id": reply_status_id,
        "reply_account_id": reply_account_id,
        #"reply_content"
        #"reply_media_type"
        #"reply_media_url"
        #"reply_tags"

        "media_type": media_type,
        "media_url": media_url,

        "mention_id": mention_id,
        "mention_username": mention_username,

        "tags": status["tags"] # 99% have no tags, <1% have tags
    }




class TruthService:
    def __init__(self, username=TRUTH_USERNAME, password=TRUTH_PASSWORD):
        self.username = username
        self.password = password

        self.client = Api(username=username, password=password)


    def get_user(self, handle='realDonaldTrump'):
        return self.client.lookup(user_handle=handle)

    def get_user_timeline(self, handle='realDonaldTrump', created_after=BEGINNING_OF_TIME):
        """returns a generator"""
        return self.client.pull_statuses(username=handle, created_after=created_after, replies=True)



if __name__ == "__main__":


    service = TruthService()
    user = service.get_user()
    #pprint(user)
    print(user.keys())

    recent_day = date.today() - timedelta(days=3)
    timeline = list(service.get_user_timeline(created_after=recent_day))
    print(len(timeline))
    #pprint(timeline[0])
    #for status in timeline:
    #    print(status.id)

    parsed_status = parse_status(timeline[0])
    pprint(parsed_status)


    breakpoint()
