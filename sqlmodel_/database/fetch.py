import typing

import pandas as pd
import sqlmodel
import sqlmodel.sql.expression

import sqlmodel_.models


def fetch_all(sqlite_filename: str,
              statement: typing.Union[sqlmodel.sql.expression.Select, sqlmodel.sql.expression.SelectOfScalar]):
    """

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        data = session.exec(statement).all()
    return data


def fetch_all_df(sqlite_filename: str,
                 statement: typing.Union[sqlmodel.sql.expression.Select, sqlmodel.sql.expression.SelectOfScalar],
                 columns: typing.List[str]) -> pd.DataFrame:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :param columns: list of column names
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

    :param sqlite_filename: file name of the sqlite database file
    :return:
    """
    statement = sqlmodel_.models.Feed().fetch_feeds()
    feeds = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return feeds


def fetch_feeds_df_from_db(sqlite_filename: str) -> pd.DataFrame:
    """

    :param sqlite_filename: file name of the sqlite database file
    :return:
    """
    statement = sqlmodel_.models.Feed().fetch_feeds()
    feeds_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=['id', 'title', 'file_url', 'download_url', 'downloaded'])
    return feeds_df


def fetch_single_feed_from_db(sqlite_filename: str, feed_id: int) -> sqlmodel_.models.Feed:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel_.models.Feed().fetch_single_feed(feed_id=feed_id)
        feed = session.exec(statement).one()
    return feed


def fetch_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedItem]:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return:
    """
    statement = sqlmodel_.models.FeedItem.find_items_for_feed(feed_id=feed_id)
    episodes = fetch_all(sqlite_filename=sqlite_filename,
                         statement=statement)
    return episodes


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return:
    """
    statement = sqlmodel_.models.FeedItem.find_items_for_feed(feed_id=feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']

    episodes_df = fetch_all_df(sqlite_filename=sqlite_filename,
                               statement=statement,
                               columns=columns)
    return episodes_df


def fetch_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedMedia]:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return:
    """
    statement = sqlmodel_.models.FeedMedia().find_media_for_feed(feed_id=feed_id)
    media = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return media


def fetch_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return:
    """
    statement = sqlmodel_.models.FeedMedia().find_media_for_feed(feed_id=feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df
