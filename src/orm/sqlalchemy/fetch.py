import typing

import sqlalchemy
import sqlalchemy.engine
import sqlalchemy.engine.row
import sqlalchemy.orm
import sqlalchemy.sql.selectable

import src.orm.sqlalchemy.database
import src.orm.sqlalchemy.models


def create_fetch_feeds_statement(columns: typing.List[str],
                                 where_cond: typing.Dict[str, typing.Any] = None) -> sqlalchemy.sql.selectable.Select:
    """
    Create statement to fetch all feeds.

    :param columns: column names to be fetched
    :param where_cond:
    :return: select statement
    """
    specific_cols = [sqlalchemy.sql.column(col) for col in columns]
    statement = sqlalchemy.sql.select(
        from_obj=src.orm.sqlalchemy.models.Feed,
        columns=specific_cols,
    )
    if where_cond:
        expressions = []
        for column, values in where_cond.items():
            if isinstance(values, str):
                expressions.append(getattr(src.orm.sqlalchemy.models.Feed, column) == values)
            elif isinstance(values, list):
                expressions.append(getattr(src.orm.sqlalchemy.models.Feed, column).in_(values))
    else:
        expressions = []
    statement = statement.where(sqlalchemy.and_(True, *expressions))
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
    Create statement to fetch all media for a feed.

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
