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
