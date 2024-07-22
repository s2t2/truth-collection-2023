import os
from datetime import datetime
from functools import cached_property

from dotenv import load_dotenv
from google.cloud import bigquery
#from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter
from pandas import DataFrame

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") # implicit check by google.cloud (and keras)

# used by child classes only, defined here for convenience
DATASET_ADDRESS = os.getenv("DATASET_ADDRESS", default="project-name.database_name") # "MY_PROJECT.MY_DATASET"


def generate_timestamp(dt=None):
    """Formats datetime object for storing in BigQuery. Uses current time by default. """
    dt = dt or datetime.now()
    return dt.strftime("%Y-%m-%d %H:%M:%S")


class BigQueryService():

    def __init__(self, client=None, dataset_address=DATASET_ADDRESS):
        self.client = client or bigquery.Client()
        self.dataset_address = dataset_address

    def execute_query(self, sql, verbose=True):
        if verbose == True:
            print(sql)
        job = self.client.query(sql)
        return job.result()

    def query_to_df(self, sql, verbose=True):
        """high-level wrapper to return a DataFrame"""
        results = self.execute_query(sql, verbose=verbose)
        records = [dict(row) for row in list(results)]
        df = DataFrame(records)
        return df

    @staticmethod
    def split_into_batches(my_list, batch_size=10_000):
        """Splits a list into evenly sized batches"""
        # h/t: https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
        for i in range(0, len(my_list), batch_size):
            yield my_list[i : i + batch_size]

    #@staticmethod
    #def generate_timestamp(dt=None):
    #    """Formats datetime object for storing in BigQuery. Uses current time by default. """
    #    dt = dt or datetime.now()
    #    return dt.strftime("%Y-%m-%d %H:%M:%S")

    def insert_records_in_batches(self, table, records):
        """
        Inserts records in batches because attempting to insert too many rows at once
            may result in google.api_core.exceptions.BadRequest: 400

        Params:
            table (table ID string, Table, or TableReference)
            records (list of dictionaries)
        """
        rows_to_insert = [list(d.values()) for d in records]
        #errors = self.client.insert_rows(table, rows_to_insert)
        errors = []
        batches = list(BigQueryService.split_into_batches(rows_to_insert, batch_size=5_000))
        for batch in batches:
            errors += self.client.insert_rows(table, batch)
        return errors

    # MIGRATIONS

    def migrate_timeline_statuses_table(self, destructive=False):
        """WARNING: DESTRUCTIVE MODE WILL DELETE THE TABLE!!!"""
        sql = ""
        if destructive:
            sql += f"DROP TABLE IF EXISTS {self.dataset_address}.timeline_statuses; "

        sql += f"""
            CREATE TABLE IF NOT EXISTS {self.dataset_address}.timeline_statuses(
                -- collect identifiers as strings for now, will convert to int later if possible for faster joins

                status_id STRING, -- v2: convert to INT64
                user_id STRING, -- v2: convert to INT64
                username STRING,
                created_at TIMESTAMP,
                lang STRING,
                content STRING, -- HTML text, for v2: also make a column of just the text, if possible

                group_id STRING, -- v2: convert to INT64
                group_slug STRING,

                reply_status_id STRING, -- v2: convert to INT64
                reply_user_id STRING, -- v2: convert to INT64

                media_id STRING, -- v2: convert to INT64
                media_type STRING,
                media_url STRING,

                mention_id STRING, -- v2: convert to INT64
                mention_username STRING,

                tags ARRAY<STRING>,

                collected_at TIMESTAMP -- FYI some metadata from us

            );
        """
        self.execute_query(sql)

    # TABLE REFERENCES

    @cached_property
    def timeline_statuses_table(self):
        return self.client.get_table(f"{self.dataset_address}.timeline_statuses") # API call



if __name__ == "__main__":

    service = BigQueryService()
    client = service.client
    print("PROJECT:", client.project)

    print("DATASETS:")
    datasets = list(client.list_datasets())
    for ds in datasets:
        #print("...", ds.project, ds.dataset_id)
        print("...", ds.reference)
