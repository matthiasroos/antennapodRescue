import typing

import pandas as pd
import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.engine.row
import sqlalchemy.orm
import sqlalchemy.sql.selectable

import src.database.fetch
import src.orm.sqlalchemy.database
import src.orm.sqlalchemy.models


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


def create_fetch_feeditems_statement(columns: typing.List[str],
                                     feed_id: int) -> sqlalchemy.sql.selectable.Select:
    """
    Create statement to fetch all feeditems for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return:
    """
    specific_cols = [sqlalchemy.sql.column(col) for col in columns]
    statement = sqlalchemy.sql.select(
        from_obj=src.orm.sqlalchemy.models.FeedItem,
        columns=specific_cols,
    ).where(src.orm.sqlalchemy.models.FeedItem.feed == feed_id)
    return statement


def create_fetch_media_statement(columns: typing.List[str],
                                 feed_id: int) -> sqlalchemy.sql.selectable.Select:
    """
    Fetch all media for a feed from db and return them as a dataframe.

    :param columns:
    :param feed_id: id of the feed
    :return: dataframe containing all media
    """
    specific_cols = [sqlalchemy.sql.column(col) if col != 'id' else src.orm.sqlalchemy.models.FeedMedia.id
                     for col in columns]
    statement = sqlalchemy.sql.select(
        from_obj=src.orm.sqlalchemy.models.FeedMedia,
        columns=specific_cols,
    ).filter(src.orm.sqlalchemy.models.FeedMedia.feeditem == src.orm.sqlalchemy.models.FeedItem.id)\
        .where(src.orm.sqlalchemy.models.FeedItem.feed == feed_id)
    return statement
