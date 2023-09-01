
import os
from pprint import pprint

from truthbrush import Api
from dotenv import load_dotenv

load_dotenv()

TRUTH_USERNAME = os.getenv("TRUTH_USERNAME")
TRUTH_PASSWORD = os.getenv("TRUTH_PASSWORD")


if __name__ == "__main__":

    username=TRUTH_USERNAME
    password=TRUTH_PASSWORD
    client = Api(username=username, password=password)

    user = client.lookup(user_handle='realDonaldTrump')
    pprint(user)
