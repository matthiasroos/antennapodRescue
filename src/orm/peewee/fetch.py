import typing

import pandas as pd
import peewee

import src.database.fetch
import src.orm.peewee.models


def get_connection(sqlite_filename: str):
    """

    :param sqlite_filename:
    :return:
    """
    return peewee.SqliteDatabase(sqlite_filename).connection()


def fetch_all_df(sqlite_filename: str,
                 statement: peewee.ModelSelect,
                 columns: typing.List[str]) -> pd.DataFrame:
    """
    Base method.
    Fetch all rows of a table from db and return a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: peewee SQL statement
    :param columns: list of column names
    :return: dataframe containing all data
    """
    connection = peewee.SqliteDatabase(sqlite_filename).connection()
    sql, params = statement.sql()
    data_df = pd.read_sql(
        sql=sql,
        con=connection,
        params=params,
        columns=columns)
    return data_df


def create_fetch_feeds_statement(columns: typing.List[str]):
    """
    Fetch all feeds from db and return them as a dataframe.

    :param columns:
    :return:
    """
    if columns:
        specific_cols = [getattr(src.orm.peewee.models.Feed, col) for col in columns]
        query = src.orm.peewee.models.Feed.select(*specific_cols)
    else:
        query = src.orm.peewee.models.Feed.select()
    return query


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int, sort_by: typing.Iterable[str] = None) -> pd.DataFrame:
    """
    Fetch all episodes for a feed from db and return them as a sorted dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :param sort_by: list of column names to sorted by
    :return: dataframe containing all episodes
    """
    sort_by = [] if sort_by is None else list(sort_by)
    statement = peewee_.models.FeedItem.select().where(peewee_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']
    print(statement.sql())
    episodes_df = fetch_all_df(sqlite_filename=sqlite_filename,
                               statement=statement,
                               columns=columns)
    episodes_df = episodes_df.sort_values(by=sort_by)
    return episodes_df


def fetch_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """
    Fetch all media for a feed from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: dataframe containing all media
    """
    statement = peewee_.models.FeedMedia.select() \
        .join(peewee_.models.FeedItem, on=(peewee_.models.FeedMedia.feeditem == peewee_.models.FeedItem.id))\
        .where(peewee_.models.FeedItem.feed == feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df