
import os
import sqlite3
#from contextlib import contextmanager
from pandas import DataFrame

EXPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "exports")

DEFAULT_FILEPATH = os.path.join(EXPORTS_DIR, "truth_collection_development.sqlite")
DB_FILEPATH = os.getenv("DB_FILEPATH") or DEFAULT_FILEPATH

DESTRUCTIVE_MODE = bool(os.getenv("DESTRUCTIVE") == "true")

#def to_table(df:DataFrame, table_name:str):
#    connection = connect(DB_FILEPATH)
#    df.to_sql(table_name, connection, if_exists='replace', index=False)
#    connection.close()


#@contextmanager
#def manage_connection(db_filepath):
#    connection = sqlite3.connect(db_filepath)
#    try:
#        yield connection
#    finally:
#        connection.close()
#
#def with_connection(func):
#    def wrapper(*args, **kwargs):
#        with manage_connection(DB_FILEPATH) as connection:
#            return func(*args, connection=connection, **kwargs)
#    return wrapper
#
#@with_connection
#def to_table(df:DataFrame, table_name:str, connection):
#    df.to_sql(table_name, connection, if_exists='replace', index=False)


class Database:

    def __init__(self, filepath=DB_FILEPATH, destructive=False):
        """A base interface into SQLite database.

        Params:
            filepath (str):
                path to the database that exists or will be created
        """
        self.destructive = bool(destructive)

        self.filepath = filepath
        print("------------------")
        print("DB FILEPATH:", os.path.abspath(self.filepath))

        self.connection = sqlite3.connect(self.filepath)
        self.connection.row_factory = sqlite3.Row
        #print("CONNECTION:", self.connection)

        self.cursor = self.connection.cursor()
        #print("CURSOR", self.cursor)

        if self.destructive:
            print("DB DESTRUCTIVE:", self.destructive)
            #self.drop_tables()
            if os.path.exists(self.filepath):
                os.remove(self.filepath)

    #def drop_table(self, table_name):
    #    self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    def insert_data(self, table_name:str, records:list):
        """Params:
            table_name (str) : name of table to insert data into
            records (list[dict]) : records to save
        """
        if not records or not any(records):
            return None

        df = DataFrame(records)
        self.insert_df(df=df, table_name=table_name)

    def insert_df(self, table_name:str, df:DataFrame):
        """Params:

            table_name (str):
                name of table to insert data into

            df (pandas.DataFrame):
                dataframe of records to save
        """
        #df.index.rename("row_id", inplace=True) # assigns a column label "id" for the index column
        #df.index += 1 # starts ids at 1 instead of 0

        # convert lists to CSV strings (b/c SQLite can't handle nested data)
        for column in df.columns:
            if df[column].apply(lambda x: isinstance(x, list)).any():
                df[column] = df[column].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

        # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
        df.to_sql(table_name, con=self.connection,
            if_exists="append", # append to existing tables (don't throw error)
            #index_label="row_id", # store unique ids for the rows, so we could count them (JK this restarts numbering at 1 for each df)
            index=False
        )







if __name__ == "__main__":

    df = DataFrame({'col1': [1, 2], 'col2': [3, 4]})

    print(df.head())
    print("INSERTING DATA...")

    db = Database()
    db.insert_df(df=df, table_name="test_exports")
