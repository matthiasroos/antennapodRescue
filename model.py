import dataclasses
import datetime
from typing import NewType


URL = NewType('URL', str)
Title = NewType('Title', str)


@dataclasses.dataclass
class AntennaPodElement:
    pass


@dataclasses.dataclass
class Feed(AntennaPodElement):
    id: int
    title: Title
    file_url: str
    dowload_url: URL
    downloaded: int


@dataclasses.dataclass
class FeedItem(AntennaPodElement):
    id: int
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
    id: int
    duration: datetime.timedelta
    download_url: URL
    downloaded: int
    filesize: int
    playback_completion_date: datetime.datetime
    feeditem: int
    played_duration: datetime.timedelta
    last_played_time: datetime.datetime


kind_mapping = {'feeds': Feed,
                'feeditems': FeedItem,
                'media': FeedMedia}
