import typing

import src.orm.peewee.models


def create_fetch_feeds_statement(columns: typing.List[str],
                                 where_cond: typing.Dict[str, typing.Any] = None):
    """
    Create statement to fetch all feeds.

    :param columns:
    :param where_cond:
    :return:
    """
    if columns:
        specific_cols = [getattr(src.orm.peewee.models.Feed, col) for col in columns]
        query = src.orm.peewee.models.Feed.select(*specific_cols)
    else:
        query = src.orm.peewee.models.Feed.select()
    if where_cond:
        expressions = []
        for column, values in where_cond.items():
            if isinstance(values, str) or isinstance(values, int):
                expressions.append(getattr(src.orm.peewee.models.Feed, column) == values)
            elif isinstance(values, list):
                expressions.append(getattr(src.orm.peewee.models.Feed, column).in_(values))
    else:
        expressions = []
    query = query.where(True, *expressions)
    return query


def create_fetch_feeditems_statement(columns: typing.List[str],
                                     feed_id: int):
    """
    Create statement to fetch all feeditems for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return:
    """
    if columns:
        specific_cols = [getattr(src.orm.peewee.models.FeedItem, col) for col in columns]
        query = src.orm.peewee.models.FeedItem.select(*specific_cols)\
            .where(src.orm.peewee.models.FeedItem.feed == feed_id)
    else:
        query = src.orm.peewee.models.FeedItem.select()\
            .where(src.orm.peewee.models.FeedItem.feed == feed_id)
    return query


def create_fetch_media_statement(columns: typing.List[str],
                                 feed_id: int):
    """
    Fetch all media for a feed from db and return them as a dataframe.

    :param columns:
    :param feed_id: id of the feed
    :return: dataframe containing all media
    """
    if columns:
        specific_cols = [getattr(src.orm.peewee.models.FeedMedia, col) for col in columns]
        query = src.orm.peewee.models.FeedMedia.select(*specific_cols) \
            .join(src.orm.peewee.models.FeedItem,
                  on=(src.orm.peewee.models.FeedMedia.feeditem == src.orm.peewee.models.FeedItem.id)) \
            .where(src.orm.peewee.models.FeedItem.feed == feed_id)
    else:
        query = src.orm.peewee.models.FeedMedia.select() \
            .join(src.orm.peewee.models.FeedItem,
                  on=(src.orm.peewee.models.FeedMedia.feeditem == src.orm.peewee.models.FeedItem.id)) \
            .where(src.orm.peewee.models.FeedItem.feed == feed_id)
    return query
