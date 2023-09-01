
import os
from pprint import pprint
from datetime import date, timedelta

from truthbrush import Api
from dotenv import load_dotenv

load_dotenv()

TRUTH_USERNAME = os.getenv("TRUTH_USERNAME")
TRUTH_PASSWORD = os.getenv("TRUTH_PASSWORD")

BEGINNING_OF_TIME = date(2022, 1, 1) # start of timeline



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
    pprint(user)

    last_week = date.today() - timedelta(days=7)
    timeline = list(service.get_user_timeline(created_after=last_week))
    print(len(timeline))
    pprint(timeline[0])
    #for tweet in timeline:
    #    print(tweet.id)
