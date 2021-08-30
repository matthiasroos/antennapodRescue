
import typing

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


class FeedMedia(sqlmodel.SQLModel, table=True):
    """
    Table FeedMedia
    """
    __tablename__ = "FeedMedia"
    id: typing.Optional[int] = sqlmodel.Field(default=None, primary_key=True)
    duration: int
    download_url: str
    downloaded: int
    filesize: int
    feeditem: int = sqlmodel.Field(default=None, foreign_key="FeedItems.id")
