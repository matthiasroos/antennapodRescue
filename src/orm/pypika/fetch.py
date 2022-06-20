import typing

import pypika


def create_fetch_feeds_statement(columns: typing.List[str],
                                 where_cond: typing.Dict[str, typing.Any] = None):
    """

    :param columns:
    :param where_cond:
    :return:
    """
    feeds = pypika.Table('Feeds')
    specific_cols = [getattr(feeds, col) for col in columns]
    query = pypika.Query.from_(feeds) \
        .select(*specific_cols)
    if where_cond:
        expressions = []
        for column, values in where_cond.items():
            if isinstance(values, str) or isinstance(values, int):
                expressions.append(getattr(feeds, column) == values)
            elif isinstance(values, list):
                expressions.append(getattr(feeds, column).isin(values))
        query = query.where(*expressions)
    return query


def create_fetch_feeditems_statement(columns: typing.List[str],
                                     feed_id: int):
    """
    Create statement to fetch all feeditems for a feed.

    :param columns:
    :param feed_id: id of the feed
    :return:
    """
    feeditems = pypika.Table('FeedItems')
    specific_cols = [getattr(feeditems, col) for col in columns]
    query = pypika.Query.from_(feeditems) \
        .select(*specific_cols) \
        .where(feeditems.feed == feed_id)
    return query


def create_fetch_media_statement(columns: typing.List[str],
                                 feed_id: int):
    """

    :param columns:
    :param feed_id:
    :return:
    """
    feeditems = pypika.Table('FeedItems')
    media = pypika.Table('FeedMedia')
    specific_cols = [getattr(media, col) for col in columns]
    query = pypika.Query.from_(media) \
        .select(*specific_cols) \
        .join(feeditems) \
        .on(media.feeditem == feeditems.id) \
        .where(feeditems.feed == feed_id)

    return query
