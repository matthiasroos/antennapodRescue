import typing

import pandas as pd
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.engine.row
import sqlalchemy.orm
import sqlalchemy.sql.selectable

import src.database.fetch
import src.orm.sqlalchemy.models


def fetch_all(sqlite_filename: str,
              statement: sqlalchemy.sql.selectable.Select) -> typing.List[sqlalchemy.engine.row.Row]:
    """
    Base method.
    Fetch all rows of a table from db and return a list of SQLAlchemy Row objects.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :return:
    """
    with sqlalchemy.orm.Session(get_engine(sqlite_filename=sqlite_filename)) as session:
        data = session.execute(statement).all()
    return data


def fetch_all_df(sqlite_filename: str,
                 statement: sqlalchemy.sql.selectable.Select,
                 columns: typing.List[str]) -> pd.DataFrame:
    """
    Base method.
    Fetch all rows of a table from db and return a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param statement: SQL query to be executed
    :param columns: list of column names
    :return: dataframe containing all data
    """
    con = get_connection(sqlite_filename=sqlite_filename)
    data_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=columns)
    con.close()
    return data_df


def fetch_feeds_from_db(sqlite_filename: str) -> typing.List[sqlalchemy.engine.row.Row]:
    """
    Fetch feeds from db and return them as a list of SQLAlchemy Row objects.

    :param sqlite_filename: file name of the sqlite database file
    :return: list of all feeds as sqlalchemy.engine.row.Row
    """
    statement = sqlalchemy.sql.select(src.orm.sqlalchemy.models.Feed)
    feeds = fetch_all(sqlite_filename=sqlite_filename,
                      statement=statement)
    return feeds


def create_fetch_feeds_statement(columns: typing.List[str]) -> sqlalchemy.sql.selectable.Select:
    """
    Create statement to fetch all feeds.

    :param columns: column names to be fetched
    :return: select statement
    """
    specific_cols = [sqlalchemy.sql.column(col) for col in columns]
    statement = sqlalchemy.sql.select(
        from_obj=src.orm.sqlalchemy.models.Feed,
        columns=specific_cols,
    )
    return statement


def fetch_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlalchemy.engine.row.Row]:
    """
    Fetch all feeditems for a feed from db and return them as a list of SQLAlchemy Row objects.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: list of all feeditems for a feed as sqlalchemy.engine.row.Row
    """
    statement = src.orm.sqlalchemy.models.FeedItem().where(src.orm.sqlalchemy.models.FeedItem.feed == feed_id)
    episodes = fetch_all(sqlite_filename=sqlite_filename,
                         statement=statement)
    return episodes


def create_fetch_feeditems_statement(columns: typing.List[str],
                                     feed_id: int) -> sqlalchemy.sql.selectable.Select:
    """
     Create statement to fetch all feeditems for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return: dataframe containing all episodes
    """
    specific_cols = [sqlalchemy.sql.column(col) for col in columns]
    statement = sqlalchemy.sql.select(
        from_obj=src.orm.sqlalchemy.models.FeedItem,
        columns=specific_cols,
    ).where(src.orm.sqlalchemy.models.FeedItem.feed == feed_id)
    return statement


def fetch_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """
    Fetch all media for a feed from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: dataframe containing all media
    """
    statement = sqlalchemy.sql.select(src.orm.sqlalchemy.models.FeedMedia) \
        .filter(src.orm.sqlalchemy.models.FeedMedia.feeditem == src.orm.sqlalchemy.models.FeedItem.id)\
        .where(src.orm.sqlalchemy.models.FeedItem.feed == feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = fetch_all_df(sqlite_filename=sqlite_filename,
                            statement=statement,
                            columns=columns)
    return media_df
