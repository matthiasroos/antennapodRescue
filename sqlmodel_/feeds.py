import typing

import requests
import sqlmodel

import sqlmodel_.database
import sqlmodel_.models


def download_xml(url: str) -> typing.Optional[bytes]:
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f'Error while loading xml from {url}: {response.status_code}')
        return None

    return response.content


def get_feeds_from_db(sqlite_filename: str) -> typing.List[sqlmodel_.models.Feed]:
    """

    :param sqlite_filename:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.Feed)
    feeds = sqlmodel_.database.fetch_all(sqlite_filename=sqlite_filename,
                                         statement=statement)
    return feeds


def get_feed_from_db(sqlite_filename: str, feed_id: int) -> sqlmodel_.models.Feed:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(sqlmodel_.models.Feed).where(sqlmodel_.models.Feed.id == feed_id)
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
