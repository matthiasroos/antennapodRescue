import datetime
import functools
import typing
import xml.etree.ElementTree as ET

import pandas as pd
import requests

import utils


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

        datetime_entry = utils.parse_pubdate(element=ep)

        item = [ep.find('title').text, int(datetime_entry), 0, ep.find('description'), ep.find('guid').text]
        episode_list.append(item)
    return pd.DataFrame(episode_list,
                        columns=['title', 'pubDate', 'read', 'description', 'item_identifier'])
