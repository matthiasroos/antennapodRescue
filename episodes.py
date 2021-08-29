import collections
import datetime
import xml.etree.ElementTree as ET

import typing

import pandas as pd
import sqlmodel


class FeedItem(sqlmodel.SQLModel, table=True):
    """
    Table FeedItems
    """
    __tablename__ = 'FeedItems'
    id: typing.Optional[int] = sqlmodel.Field(default=None, primary_key=True)
    title: str
    pubDate: int
    read: int
    link: str
    description: str
    feed: int
    item_identifier: str
    image_url: str


def parse_xml_for_feeditems(xml: bytes) -> typing.List[FeedItem]:
    """
    Parse a XML as bytes to obtain all included .

    :param xml:
    :return:
    """
    root = ET.fromstring(xml)
    episode_list = []
    for ep in root.iter(tag='item'):
        item = FeedItem(title=ep.find('title').text,
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
        datetime_entry = datetime.datetime.strptime(ep.find('pubDate').text, datetime_format).timestamp() * 1000
        item = [ep.find('title').text, int(datetime_entry), 0, ep.find('link').text, ep.find('description'),
                ep.find('guid').text]
        episode_list.append(item)
    return pd.DataFrame(episode_list,
                        columns=['title', 'pubDate', 'read', 'link', 'description', 'item_identifier'])


def get_feeditems_from_db(sqlite_filename: str, feed_id: int) -> typing.List[FeedItem]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(FeedItem).where(FeedItem.feed == feed_id)
        episodes = session.exec(statement).all()
    return episodes


def get_episodes_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    con = engine.connect()
    statement = sqlmodel.select(FeedItem).where(FeedItem.feed == feed_id)
    episodes_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url'])
    con.close()
    return episodes_df
