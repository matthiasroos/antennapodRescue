
import pandas as pd
import pypika

import database.fetch
import sqlalchemy_.database.fetch


def fetch_episodes_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """
    Fetch all episodes for a feed from db and return them as a dataframe.

    :param sqlite_filename: file name of the sqlite database file
    :param feed_id: id of the feed
    :return: dataframe containing all episodes
    """
    feeditems = pypika.Table('FeedItems')
    query = pypika.Query.from_(feeditems) \
        .select(feeditems.id, feeditems.title, feeditems.pubDate, feeditems.read, feeditems.link, feeditems.link,
                feeditems.description, feeditems.feed, feeditems.item_identifier, feeditems.image_url) \
        .where(feeditems.feed == feed_id)
    connection = sqlalchemy_.database.fetch.get_connection(sqlite_filename=sqlite_filename)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']
    feeditems_df = database.fetch.fetch_all_df(connection=connection,
                                               statement=query,
                                               columns=columns)
    return feeditems_df
