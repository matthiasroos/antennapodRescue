import typing

import pandas as pd
import sqlalchemy
import sqlalchemy.orm

import sqlalchemy_.models


def fetch_all(sqlite_filename: str, statement) -> typing.List:
    """
    Base method.
    Fetch all rows of a table from db and return a list of SQLAlchemy Row objects.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :return:
    """
    engine = sqlalchemy.create_engine(f'sqlite+pysqlite:///{sqlite_filename}')
    with sqlalchemy.orm.Session(engine) as session:
        data = session.execute(statement).all()
    return data


def fetch_all_df(sqlite_filename: str, statement, columns: typing.List[str]) -> pd.DataFrame:
    """
    Base method.
    Fetch all rows of a table from db and return a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :param columns: list of column names
    :return:
    """
    engine = sqlalchemy.create_engine(f'sqlite+pysqlite:///{sqlite_filename}')
    con = engine.connect()
    data_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=columns)
    con.close()
    return data_df


def fetch_feeds_from_db(sqlite_filename: str) -> typing.List:
    """

    :param sqlite_filename: file name of the sqlite database file
    :return:
    """
    statement = sqlalchemy.sql.select(sqlalchemy_.models.Feed)
    feeds = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return feeds


def fetch_feeds_df_from_db(sqlite_filename: str) -> pd.DataFrame:
    """
    Fetch all feeds from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :return:
    """
    statement = sqlalchemy.sql.select(sqlalchemy_.models.Feed)
    feeds_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=['id', 'title', 'file_url', 'download_url', 'downloaded', 'feeditems'])
    return feeds_df


def fetch_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id:
    :return:
    """
    statement = sqlalchemy.sql.select(sqlalchemy_.models.FeedItem).where(sqlalchemy_.models.FeedItem.feed == feed_id)
    episodes = fetch_all(sqlite_filename=sqlite_filename,
                         statement=statement)
    return episodes


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int, sort_by: typing.List[str] = None) -> pd.DataFrame:
    """
    Fetch all episodes for a feed from db and return them as a sorted dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :param sort_by: list of column names to sorted by
    :return: dataframe containing all episodes
    """
    sort_by = [] if not sort_by else sort_by
    statement = sqlalchemy.sql.select(sqlalchemy_.models.FeedItem).where(sqlalchemy_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']

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
    statement = sqlalchemy.sql.select(sqlalchemy_.models.FeedMedia) \
        .filter(sqlalchemy_.models.FeedMedia.feeditem == sqlalchemy_.models.FeedItem.id)\
        .where(sqlalchemy_.models.FeedItem.feed == feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df
