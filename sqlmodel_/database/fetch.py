import typing

import pandas as pd
import sqlmodel

import sqlmodel_.models


def fetch_all(sqlite_filename: str, statement):
    """

    :param sqlite_filename:
    :param statement:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        data = session.exec(statement).all()
    return data


def fetch_all_df(sqlite_filename: str, statement, columns: typing.List[str]) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param statement:
    :param columns:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    con = engine.connect()
    data_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=columns)
    con.close()
    return data_df


def fetch_feeds_from_db(sqlite_filename: str) -> typing.List[sqlmodel_.models.Feed]:
    """

    :param sqlite_filename:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.Feed)
    feeds = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return feeds


def fetch_single_feed_from_db(sqlite_filename: str, feed_id: int) -> sqlmodel_.models.Feed:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(sqlmodel_.models.Feed).where(sqlmodel_.models.Feed.id == feed_id)
        feed = session.exec(statement).one()
    return feed


def fetch_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedItem]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedItem).where(sqlmodel_.models.FeedItem.feed == feed_id)
    episodes = fetch_all(sqlite_filename=sqlite_filename,
                         statement=statement)
    return episodes


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedItem).where(sqlmodel_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']

    episodes_df = fetch_all_df(sqlite_filename=sqlite_filename,
                               statement=statement,
                               columns=columns)
    return episodes_df


def fetch_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedMedia]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedMedia) \
        .filter(sqlmodel_.models.FeedMedia.feeditem == sqlmodel_.models.FeedItem.id) \
        .where(sqlmodel_.models.FeedItem.feed == feed_id)
    media = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return media


def fetch_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedMedia) \
        .filter(sqlmodel_.models.FeedMedia.feeditem == sqlmodel_.models.FeedItem.id)\
        .where(sqlmodel_.models.FeedItem.feed == feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df
