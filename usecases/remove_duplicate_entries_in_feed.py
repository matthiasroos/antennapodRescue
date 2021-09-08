"""
This script is used to remove duplicate episodes in a podcast feed,
which were introduced into the AntennaPod database
as the feed owner changed all URLs from http to https.

It relies on a list of assumptions:
 * unchanged title of the items ('title' in FeedItems)
 * unchanged publication date of the items ('pubDate' in FeedItems)
 * unchanged file size of the media ('filesize' in FeedMedia)
to identify duplicate episodes.
The validity of these assumptions will vary very strongly by the specific podcast feed.
Furthermore, it is assumed that the same items are identified as duplicate in FeedItems and FeedMedia,
as they are identified independent from each other.

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

import aiosql_.database


def identify_duplicate_feeditems(feed_items_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify duplicate feed items within a dataframe by looking at the columns 'title' and 'pubDate'.

    :param feed_items_df: dataframe containing all feed items for a podcast feed
    :return: dataframe containing only the duplicates, sorted by 'title'
    """
    duplicates = feed_items_df.duplicated(subset=['title', 'pubDate'], keep=False)
    return feed_items_df[duplicates].copy().sort_values(by='title')


def identify_duplicate_media(media_df: pd.DataFrame) -> pd.DataFrame:
    """
    Identify duplicate media within a dataframe by looking at the column 'filesize'.

    :param media_df: dataframe containing all media for a podcast feed
    :return: dataframe containing only the duplicates, sorted by 'title'
    """
    duplicates = media_df.duplicated(subset=['filesize'], keep=False)
    return media_df[duplicates].copy().sort_values(by='filesize')


def consolidate_feeditems(duplicate_items_df: pd.DataFrame) -> typing.Tuple[pd.DataFrame, typing.List[str]]:
    """
    Consolidate duplicate feed items.

    Feed items with the lower id (older) are marked for update,
    feed items with the higher id (newer) are marked for deletion.

    The columns 'link', 'description', 'item_identifier' and 'image_url' of the older feed item
    are updated with the respective values of the newer feed item.

    :param duplicate_items_df: dataframe containing all duplicate feed items
    :return: tuple:
        * dataframe containing the feed items to be updated,
        * list of feed items to be deleted
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
    Consolidate duplicate media.

    Media with the lower id (older) are marked for update,
    media with the higher id (newer) are marked for deletion.

    The column 'download_url' of the older media is updated with the respective value of the newer media.

    :param duplicate_media_df: dataframe containing all duplicate media
    :return: tuple:
        * dataframe containing the media to be updated,
        * list of media to be deleted
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

    connection = aiosql_.database.get_connection(file_name=file_name)

    feed_items = aiosql_.database.fetch_items_for_feed(connection=connection, feed_number=feed_number)
    duplicate_items = identify_duplicate_feeditems(feed_items_df=feed_items)
    items_to_update, items_to_delete = consolidate_feeditems(duplicate_items_df=duplicate_items)

    media = aiosql_.database.fetch_media_for_feed(connection=connection, feed_number=feed_number)
    duplicate_media = identify_duplicate_media(media_df=media)
    media_to_update, media_to_delete = consolidate_media(duplicate_media_df=duplicate_media)

    aiosql_.database.write_feeditems_and_media_to_db(connection=connection,
                                                     items_to_update_df=items_to_update,
                                                     items_to_delete=items_to_delete,
                                                     media_to_update_df=media_to_update,
                                                     media_to_delete=media_to_delete)
    connection.close()
