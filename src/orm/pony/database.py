import sqlite3
import typing

import pandas as pd
import pony.orm

import src.database.feeds
import src.orm.pony.models


def get_connection(file_name: str) -> sqlite3.Connection:
    """

    :param file_name:
    :return:
    """
    db = src.orm.pony.models.db
    try:
        db.bind(provider='sqlite', filename=file_name)
        db.generate_mapping()
    except pony.orm.core.BindingError:
        pass
    connection = db.get_connection()
    return connection


@pony.orm.db_session()
def fetch_feeds() -> pd.DataFrame:
    """

    :return:
    """
    query = pony.orm.select(feed for feed in src.orm.pony.models.Feed)
    columns = src.database.feeds.get_feed_standard_columns()
    feeds = [[getattr(f, col) for col in columns] for f in query]
    feeds_df = pd.DataFrame(feeds, columns=columns)
    return feeds_df


def fetch_kind(kind: str,
               connection: sqlite3.Connection,
               where_cond: typing.Dict[str, typing.Any] = None,
               ) -> pd.DataFrame:
    """

    :param kind:
    :param connection:
    :param where_cond:
    :return:
    """
    if kind == 'feeds':
        elements_df = fetch_feeds()
    else:
        elements_df = pd.DataFrame()
    return elements_df
