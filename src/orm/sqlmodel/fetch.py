import typing

import pandas as pd
import sqlmodel
import sqlmodel.sql.expression

import src.database.fetch
import src.orm.sqlmodel.database
import src.orm.sqlmodel.models


def fetch_all(sqlite_filename: str,
              statement: typing.Union[sqlmodel.sql.expression.Select, sqlmodel.sql.expression.SelectOfScalar]) \
        -> typing.List:
    """
    Base method.
    Fetch all rows of a table from db and return them as a list.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :return:
    """
    engine = src.orm.sqlmodel.database.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        data = session.exec(statement).all()
    return data


def fetch_feeds_from_db(sqlite_filename: str) -> typing.List[src.orm.sqlmodel.models.Feed]:
    """
    Fetch all feeds from db and return them as a list of Feed objects.

    :param sqlite_filename: file name of the sqlite database file
    :return: list of feed data as Feed objects
    """
    statement = src.orm.sqlmodel.models.Feed().fetch_feeds()
    feeds = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return feeds


def create_fetch_feeds_statement(columns: typing.List[str]) -> typing.Union[sqlmodel.sql.expression.Select,
                                                                            sqlmodel.sql.expression.SelectOfScalar]:
    """
    Create statement to fetch all feeds.

    :param columns:
    :return: dataframe containing all feeds
    """
    statement = src.orm.sqlmodel.models.Feed().fetch_feeds(columns=columns)
    return statement


def fetch_single_feed_from_db(sqlite_filename: str, feed_id: int) -> src.orm.sqlmodel.models.Feed:
    """
    Fetch single from db and return it as a Feed object.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: feed data as Feed object
    """
    engine = src.orm.sqlmodel.database.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        statement = src.orm.sqlmodel.models.Feed().fetch_single_feed(feed_id=feed_id)
        feed = session.exec(statement).one()
    return feed


def create_fetch_feeditems_statement(columns: typing.List[str],
                                     feed_id: int) -> typing.Union[sqlmodel.sql.expression.Select,
                                                                   sqlmodel.sql.expression.SelectOfScalar]:
    """
    Create statement to fetch all feeditems for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return:
    """
    statement = src.orm.sqlmodel.models.FeedItem.fetch_feeditems_for_feed(feed_id=feed_id,
                                                                          columns=columns)
    return statement


def fetch_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[src.orm.sqlmodel.models.FeedMedia]:
    """
    Fetch all media for a feed from db and return them as a list of FeedMedia objects.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: list of feed media as FeedMedia objects
    """
    statement = src.orm.sqlmodel.models.FeedMedia().find_media_for_feed(feed_id=feed_id)
    media = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return media


def fetch_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """
    Fetch all media for a feed from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: dataframe containing all media
    """
    statement = src.orm.sqlmodel.models.FeedMedia().find_media_for_feed(feed_id=feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']

    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df
