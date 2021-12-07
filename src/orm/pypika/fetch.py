import typing

import pypika


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
