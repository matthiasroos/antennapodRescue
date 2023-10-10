import dataclasses
import datetime
import typing
from typing import NewType

import sqlalchemy.engine


URL = NewType('URL', str)
Title = NewType('Title', str)


@dataclasses.dataclass
class AntennaPodElement:
    id: int


@dataclasses.dataclass
class Feed(AntennaPodElement):
    title: Title
    file_url: str
    download_url: URL
    downloaded: int


@dataclasses.dataclass
class FeedItem(AntennaPodElement):
    title: Title
    pubDate: datetime.datetime
    read: int
    link: URL
    description: str
    feed: int
    item_identifier: str
    image_url: URL


@dataclasses.dataclass
class FeedMedia(AntennaPodElement):
    duration: datetime.timedelta
    download_url: URL
    downloaded: bool
    filesize: int
    playback_completion_date: datetime.datetime
    feeditem: int
    played_duration: datetime.timedelta
    last_played_time: datetime.datetime


kind_mapping = {'feeds': Feed,
                'feeditems': FeedItem,
                'media': FeedMedia}


def create(kind: str,
           data: typing.Union[list[sqlalchemy.engine.Row],
                              list[tuple[typing.Any]]],
           columns: list[str]) -> list[typing.Union[Feed, FeedItem, FeedMedia]]:
    """
    Create a list of AntennaPodElements.

    :param kind:
    :param data:
    :param columns:
    :return:
    """
    model_class = kind_mapping[kind]
    return [model_class(**{col: d for d, col in zip(dt, columns)}) for dt in data]
