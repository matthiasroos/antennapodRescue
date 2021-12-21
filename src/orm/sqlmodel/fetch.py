import typing

import sqlmodel.sql.expression

import src.orm.sqlmodel.database
import src.orm.sqlmodel.models


def create_fetch_feeds_statement(columns: typing.List[str],
                                 where_cond: typing.Dict[str, typing.Any] = None) \
        -> typing.Union[sqlmodel.sql.expression.Select,
                        sqlmodel.sql.expression.SelectOfScalar]:
    """
    Create statement to fetch all feeds.

    :param columns:
    :param where_cond:
    :return:
    """
    statement = src.orm.sqlmodel.models.Feed().fetch_feeds(columns=columns,
                                                           where_cond=where_cond)
    return statement


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


def create_fetch_media_statement(columns: typing.List[str],
                                 feed_id: int) -> typing.Union[sqlmodel.sql.expression.Select,
                                                               sqlmodel.sql.expression.SelectOfScalar]:
    """
    Create statement to fetch all media for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return:
    """
    statement = src.orm.sqlmodel.models.FeedMedia().fetch_media_for_feed(feed_id=feed_id,
                                                                         columns=columns)
    return statement
