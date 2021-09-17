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
    sql, params = statement.sql()
    data_df = pd.read_sql(
        sql=sql,
        con=connection,
        params=params,
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


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int, sort_by: typing.List[str] = None) -> pd.DataFrame:
    """


    :param sqlite_filename: file name of the sqlite database file
    :param feed_id:
    :param sort_by:
    :return:
    """
    sort_by = [] if not sort_by else sort_by
    statement = peewee_.models.FeedItem.select().where(peewee_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']
    print(statement.sql())
    episodes_df = fetch_all_df(sqlite_filename=sqlite_filename,
                               statement=statement,
                               columns=columns)
    episodes_df = episodes_df.sort_values(by=sort_by)
    return episodes_df
