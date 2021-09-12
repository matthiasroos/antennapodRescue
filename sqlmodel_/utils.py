
import pandas as pd

import sqlmodel_.database.fetch
import sqlmodel_.rssfeed.feed


def get_xml_for_feed(sqlite_filename: str, feed_id: int) -> bytes:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    feed = sqlmodel_.database.fetch.fetch_single_feed_from_db(sqlite_filename=sqlite_filename, feed_id=feed_id)
    xml = sqlmodel_.rssfeed.feed.download_xml(url=feed.download_url)
    return xml


def get_episodes_df_for_feed(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    feed = sqlmodel_.database.fetch.fetch_single_feed_from_db(sqlite_filename=sqlite_filename, feed_id=feed_id)
    xml = sqlmodel_.rssfeed.feed.download_xml(url=feed.download_url)
    episodes_xml_df = pd.DataFrame()
    if xml:
        episodes_xml_df = sqlmodel_.rssfeed.feed.parse_xml_for_episodes_df(xml=xml)
    return episodes_xml_df
