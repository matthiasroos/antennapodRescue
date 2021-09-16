import typing

import pandas as pd
import peewee

import peewee_.models


def fetch_all_df(sqlite_filename: str,
                 statement: peewee.ModelSelect,
                 columns: typing.List[str]) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param statement:
    :param columns:
    :return:
    """
    connection = peewee.SqliteDatabase(sqlite_filename).connection()
    data_df = pd.read_sql(
        sql=statement.sql()[0],
        con=connection,
        columns=columns)
    return data_df


def fetch_feeds_df_from_db(sqlite_filename: str) -> pd.DataFrame:
    """
    Fetch all feeds from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :return:
    """
    statement = peewee_.models.Feed.select()
    feeds_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=['id', 'title', 'file_url', 'download_url', 'downloaded', 'feeditems'])
    return feeds_df
