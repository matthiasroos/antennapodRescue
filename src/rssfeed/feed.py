import functools
import typing
import xml.etree.ElementTree as ET

import pandas as pd
import requests

import src.database.model
import src.utils


@functools.lru_cache(None)
def download_xml(url: str) -> typing.Optional[bytes]:
    """
    Download the XML file of a feed and return its content.

    :param url: URL of the XML file
    :return: content of the XML file as bytes
    """
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f'Error while loading xml from {url}: {response.status_code}')
        return None

    return response.content


def parse_xml_for_episodes_df(xml: bytes) -> pd.DataFrame:
    """
    Parse XML content and return it as a dataframe.

    :param xml: XML content as bytes
    :return: dataframe containing some information from the XML
    """
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):

        datetime_entry = src.utils.parse_pubdate(element=ep)

        item = [ep.find('title').text, int(datetime_entry), 0, ep.find('description').text, ep.find('guid').text]
        episode_list.append(item)
    return pd.DataFrame(episode_list,
                        columns=['title', 'pubDate', 'read', 'description', 'item_identifier'])


def parse_xml_for_episodes_list(xml: bytes) -> list[model.FeedItem]:
    """

    :param xml:
    :return:
    """
    root = ET.fromstring(xml)
    episodes = []
    for ep in root.iter(tag='item'):

        item = src.database.model.FeedItem(
            id=None,
            title=ep.find('title').text,
            pubDate=src.utils.parse_pubdate(element=ep),
            read=None,
            link=None,
            description=ep.find('description').text,
            feed=None,
            item_identifier=ep.find('guid').text,
            image_url=None)
        episodes.append(item)

    return episodes
