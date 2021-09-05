import typing

import pandas as pd
import sqlalchemy
import sqlalchemy.orm

import sqlalchemy_.models


def fetch_all(sqlite_filename: str, statement):
    """

    :param sqlite_filename:
    :param statement:
    :return:
    """
    engine = sqlalchemy.create_engine(f'sqlite+pysqlite:///{sqlite_filename}')
    with sqlalchemy.orm.Session(engine) as session:
        data = session.execute(statement).all()
    return data


def fetch_all_df(sqlite_filename: str, statement, columns: typing.List[str]) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param statement:
    :param columns:
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


def fetch_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlalchemy.sql.select(sqlalchemy_.models.FeedItem).where(sqlalchemy_.models.FeedItem.feed == feed_id)
    episodes = fetch_all(sqlite_filename=sqlite_filename,
                         statement=statement)
    return episodes


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int, sort_by: typing.List[str] = None) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :param sort_by:
    :return:
    """
    sort_by = [] if not sort_by else sort_by
    statement = sqlalchemy.sql.select(sqlalchemy_.models.FeedItem).where(sqlalchemy_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']

    episodes_df = fetch_all_df(sqlite_filename=sqlite_filename,
                               statement=statement,
                               columns=columns)
    episodes_df = episodes_df.sort_values(by=sort_by)
    return episodes_df
