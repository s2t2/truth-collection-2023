
import os
from time import sleep
from dotenv import load_dotenv

load_dotenv()

APP_ENV = os.getenv("APP_ENV", default="development")


def server_sleep(seconds=None):
    seconds = seconds or (24 * 60 * 60) # 24 hours
    if APP_ENV == "production":
        print("SERVER SLEEPING...")
        sleep(seconds)
