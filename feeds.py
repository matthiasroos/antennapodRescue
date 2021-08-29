import typing

import requests
import sqlmodel


class Feed(sqlmodel.SQLModel, table=True):
    """
    Table Feeds
    """
    __tablename__ = 'Feeds'
    id: typing.Optional[int] = sqlmodel.Field(default=None, primary_key=True)
    title: str
    file_url: str
    download_url: str
    downloaded: int


def download_xml(url: str) -> typing.Optional[bytes]:
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f'Error while loading xml from {url}: {response.status_code}')
        return None

    return response.content


def get_feeds_from_db(sqlite_filename: str) -> typing.List[Feed]:
    """

    :param sqlite_filename:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(Feed)
        feeds = session.exec(statement).all()
    return feeds


def get_feed_from_db(sqlite_filename: str, feed_id: int) -> Feed:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(Feed).where(Feed.id == feed_id)
        feed = session.exec(statement).one()
    return feed


def get_xml_for_feed(sqlite_filename: str, feed_id: int) -> bytes:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    feed = get_feed_from_db(sqlite_filename=sqlite_filename, feed_id=feed_id)
    xml = download_xml(url=feed.download_url)
    return xml
