import typing

import requests
import sqlmodel

import sqlmodel_.database.fetch
import sqlmodel_.models


def download_xml(url: str) -> typing.Optional[bytes]:
    response = requests.get(url=url)
    if response.status_code != 200:
        print(f'Error while loading xml from {url}: {response.status_code}')
        return None

    return response.content


def get_xml_for_feed(sqlite_filename: str, feed_id: int) -> bytes:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    feed = sqlmodel_.database.fetch.fetch_feed_from_db(sqlite_filename=sqlite_filename, feed_id=feed_id)
    xml = download_xml(url=feed.download_url)
    return xml
