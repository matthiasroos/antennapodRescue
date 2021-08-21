"""
This script is used to consolidate duplicate episodes in a podcast feed,
which were introduced into the AntennaPod database
as the feed owner changed all URLs from http to https.

It relies on a list of assumptions:
 * unchanged title of the items ('title' in FeedItems)
 * unchanged publication date of the items ('pubDate' in FeedItems)
 * unchanged file size of the media ('filesize' in FeedMedia)
to identify duplicate episodes.

Every field of the "new" episodes containing a https URL is written to the "old" episodes.
The "new" episodes are deleted.

Updated columns per table:
* FeedItems:
    * link
    * description
    * item_identifier
    * image_url

* FeedMedia:
    * download_url


"""

import typing

import pandas as pd

import database


def identify_duplicate_feeditems(feed_items_df: pd.DataFrame) -> pd.DataFrame:
    """

    :param feed_items_df:
    :return:
    """
    duplicates = feed_items_df.duplicated(subset=['title', 'pubDate'], keep=False)
    return feed_items_df[duplicates].copy().sort_values(by='title')


def identify_duplicate_media(media_df: pd.DataFrame) -> pd.DataFrame:
    """

    :param media_df:
    :return:
    """
    duplicates = media_df.duplicated(subset=['filesize'], keep=False)
    return media_df[duplicates].copy().sort_values(by='filesize')


def consolidate_feeditems(duplicate_items_df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, typing.List[str]]:
    """

    :param duplicate_items_df:
    :return:
    """
    unique_titles = duplicate_items_df['title'].unique()

    items_to_be_updated = []
    items_to_be_deleted = []
    for title in unique_titles:
        two_row_df = duplicate_items_df[duplicate_items_df['title'] == title].copy()
        two_row_df = two_row_df.sort_values(by='id')
        assert len(two_row_df) == 2
        major_row = two_row_df.iloc[0]
        minor_row = two_row_df.iloc[1]

        major_row['link'] = minor_row['link']
        major_row['description'] = minor_row['description']
        major_row['item_identifier'] = minor_row['item_identifier']
        major_row['image_url'] = minor_row['image_url']

        items_to_be_updated.append(major_row)
        items_to_be_deleted.append(minor_row['id'])

    items_to_update_df = pd.concat(items_to_be_updated, axis=1).T
    return items_to_update_df, items_to_be_deleted


def consolidate_media(duplicate_media_df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, typing.List[str]]:
    """

    :param duplicate_media_df:
    :return:
    """
    unique_filesizes = duplicate_media_df['filesize'].unique()

    media_to_be_updated = []
    media_to_be_deleted = []
    for filesize in unique_filesizes:
        two_row_df = duplicate_media_df[duplicate_media_df['filesize'] == filesize].copy()
        two_row_df = two_row_df.sort_values(by='id')
        assert len(two_row_df) == 2
        major_row = two_row_df.iloc[0]
        minor_row = two_row_df.iloc[1]

        major_row['download_url'] = minor_row['download_url']

        media_to_be_updated.append(major_row)
        media_to_be_deleted.append(minor_row['id'])

    media_to_update_df = pd.concat(media_to_be_updated, axis=1).T
    return media_to_update_df, media_to_be_deleted


if __name__ == '__main__':

    file_name = '<to be specified>'
    feed_number = 2  # needs to be changed accordingly

    connection = database.get_connection(file_name=file_name)

    feed_items = database.fetch_items_for_feed(connection=connection, feed_number=feed_number)
    duplicate_items = identify_duplicate_feeditems(feed_items_df=feed_items)
    items_to_update, items_to_delete = consolidate_feeditems(duplicate_items_df=duplicate_items)

    media = database.fetch_media_for_feed(connection=connection, feed_number=feed_number)
    duplicate_media = identify_duplicate_media(media_df=media)
    media_to_update, media_to_delete = consolidate_media(duplicate_media_df=duplicate_media)

    database.write_feeditems_and_media_to_db(connection=connection,
                                             items_to_update_df=items_to_update,
                                             items_to_delete=items_to_delete,
                                             media_to_update_df=media_to_update,
                                             media_to_delete=media_to_delete)
    connection.close()
