import datetime
import typing
import xml.etree.ElementTree as ET

import sqlmodel_.database.fetch
import sqlmodel_.models
import utils


def parse_xml_for_feeditems(xml: bytes) -> typing.List[sqlmodel_.models.FeedItem]:
    """
    Parse a XML as bytes to obtain all included .

    :param xml:
    :return:
    """
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):

        datetime_entry = utils.parse_pubdate(element=ep)

        item = sqlmodel_.models.FeedItem(title=ep.find('title').text,
                                         pubDate=datetime_entry,
                                         read=0,
                                         link=ep.find('link').text,
                                         description=ep.find('description'),
                                         item_identifier=ep.find('guid').text
                                         )
        episode_list.append(item)
    return episode_list
