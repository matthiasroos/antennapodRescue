import datetime
import typing
import xml.etree.ElementTree as ET

import pandas as pd
import requests

import sqlmodel_.database.fetch
import sqlmodel_.models


def download_xml(url: str) -> typing.Optional[bytes]:
    """

    :param url:
    :return:
    """
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f'Error while loading xml from {url}: {response.status_code}')
        return None

    return response.content


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
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):
        try:
            datetime_format_1 = '%a, %d %b %Y %H:%M:%S %z'
            datetime_entry = datetime.datetime.strptime(ep.find('pubDate').text, datetime_format_1).timestamp() * 1000
        except ValueError:
            datetime_format_2 = '%a, %d %b %Y %H:%M:%S %Z'
            datetime_entry = datetime.datetime.strptime(ep.find('pubDate').text, datetime_format_2).timestamp() * 1000

        item = [ep.find('title').text, int(datetime_entry), 0, ep.find('description'), ep.find('guid').text]
        episode_list.append(item)
    return pd.DataFrame(episode_list,
                        columns=['title', 'pubDate', 'read', 'description', 'item_identifier'])
