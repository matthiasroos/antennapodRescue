
import sqlite3
import typing

import aiosql
import pandas as pd

queries = aiosql.from_path('aiosql_/antennapod.sql', 'sqlite3')


def get_connection(file_name: str) -> sqlite3.Connection:
    """
    Create a connection to a sqlite db file.

    :param file_name: name of the file
    :return: Connection
    """
    connection = sqlite3.connect(file_name)
    return connection


def fetch_feeds(connection: sqlite3.Connection) -> pd.DataFrame:
    """
    Fetch all feeds from the database.

    :param connection: sqlite3 connection
    :return: dataframe containing all feed
    """
    with queries.fetch_all_feeds_cursor(connection) as cursor:
        columns = [column_info[0] for column_info in cursor.description]
        feeds = cursor.fetchall()

    feeds_df = pd.DataFrame(feeds)
    feeds_df.columns = columns
    return feeds_df


def fetch_items_for_feed(connection: sqlite3.Connection,
                         feed_number: int) -> pd.DataFrame:
    """
    Fetch all items for one feed from the database.

    :param connection: sqlite3 connection
    :param feed_number: internal number of the feed
    :return: dataframe containing all items of a specific feed
    """
    with queries.fetch_items_from_feed_cursor(connection, feed=feed_number) as cursor:
        columns = [column_info[0] for column_info in cursor.description]
        feed_items = cursor.fetchall()

    feed_items_df = pd.DataFrame(feed_items)
    feed_items_df.columns = columns
    return feed_items_df


def fetch_media_for_items(connection: sqlite3.Connection,
                          feeditems: typing.List[str]) -> pd.DataFrame:
    """
    Fetch all media for a list of feeditems.

    :param connection: sqlite3 connection
    :param feeditems: list of feeditem IDs
    :return: dataframe containing all media for the specified feeditems
    """
    media_list = []
    for feeditem in feeditems:
        with queries.fetch_media_cursor(connection, feeditem=feeditem) as cursor:
            columns = [column_info[0] for column_info in cursor.description]
            media = cursor.fetchall()
            media_list.append(pd.DataFrame(media))

    media_df = pd.concat(media_list)
    media_df.columns = columns
    return media_df


def fetch_media_for_feed(connection: sqlite3.Connection,
                         feed_number: int) -> pd.DataFrame:
    """
    Fetch all media for a specific feed.

    :param connection: sqlite3 connection
    :param feed_number: internal number of the feed
    :return: dataframe containing all media for the specified feed
    """
    feed_items_df = fetch_items_for_feed(connection=connection, feed_number=feed_number)
    feeditems = feed_items_df['id'].tolist()
    media_df = fetch_media_for_items(connection=connection,
                                     feeditems=feeditems)
    return media_df


def write_feeditems_and_media_to_db(connection: sqlite3.Connection,
                                    items_to_update_df: pd.DataFrame,
                                    items_to_delete: typing.List[str],
                                    media_to_update_df: pd.DataFrame,
                                    media_to_delete: typing.List[str]) -> None:
    """
    Write changes back to the database.

    :param connection: sqlite3 connection
    :param items_to_update_df: dataframe containing all feeditems to be updated
        updated columns: link, description, item_identifier, image_url
    :param items_to_delete: list containing feeditem_ids for all feeditems to be deleted
    :param media_to_update_df: dataframe containing all media to be updated
        updated columns: download_url
    :param media_to_delete: list containing all media to be deleted
    :return:
    """
    for _, it_up in items_to_update_df.iterrows():
        queries.update_feeditems(conn=connection,
                                 link=it_up['link'],
                                 description=it_up['description'],
                                 item_identifier=it_up['item_identifier'],
                                 image_url=it_up['image_url'],
                                 feeditem_id=int(it_up['id']))
    for item_id in items_to_delete:
        queries.delete_feeditem(conn=connection,
                                feeditem_id=int(item_id))

    for _, md_up in media_to_update_df.iterrows():
        queries.update_media(conn=connection,
                             download_url=md_up['download_url'],
                             feeditem=int(md_up['feeditem']))

    for item_id in media_to_delete:
        queries.delete_media(conn=connection,
                             feeditem=int(item_id))

    connection.commit()
