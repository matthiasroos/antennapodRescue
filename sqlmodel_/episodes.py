import datetime
import typing
import xml.etree.ElementTree as ET

import pandas as pd
import sqlmodel

import sqlmodel_.database
import sqlmodel_.models


def parse_xml_for_feeditems(xml: bytes) -> typing.List[sqlmodel_.models.FeedItem]:
    """
    Parse a XML as bytes to obtain all included .

    :param xml:
    :return:
    """
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):
        item = sqlmodel_.models.FeedItem(title=ep.find('title').text,
                                         pubDate=ep.find('pubDate').text,
                                         read=0,
                                         link=ep.find('link').text,
                                         description=ep.find('description'),
                                         item_identifier=ep.find('guid').text
                                         )
        episode_list.append(item)
    return episode_list


def parse_xml_for_episodes_df(xml: bytes) -> pd.DataFrame:
    """

    :param xml:
    :return:
    """
    datetime_format = '%a, %d %b %Y %H:%M:%S %z'
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):
        try:
            datetime_format_1 = '%a, %d %b %Y %H:%M:%S %z'
            datetime_entry = datetime.datetime.strptime(ep.find('pubDate').text, datetime_format_1).timestamp() * 1000
        except ValueError:
            datetime_format_2 = '%a, %d %b %Y %H:%M:%S %Z'
            datetime_entry = datetime.datetime.strptime(ep.find('pubDate').text, datetime_format_2).timestamp() * 1000

        item = [ep.find('title').text, int(datetime_entry), 0, ep.find('link').text, ep.find('description'),
                ep.find('guid').text]
        episode_list.append(item)
    return pd.DataFrame(episode_list,
                        columns=['title', 'pubDate', 'read', 'link', 'description', 'item_identifier'])


def get_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedItem]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedItem).where(sqlmodel_.models.FeedItem.feed == feed_id)
    episodes = sqlmodel_.database.fetch_all(sqlite_filename=sqlite_filename,
                                            statement=statement)
    return episodes


def get_episodes_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedItem).where(sqlmodel_.models.FeedItem.feed == feed_id)
    columns = ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']

    episodes_df = sqlmodel_.database.fetch_all_df(sqlite_filename=sqlite_filename,
                                                  statement=statement,
                                                  columns=columns)
    return episodes_df
