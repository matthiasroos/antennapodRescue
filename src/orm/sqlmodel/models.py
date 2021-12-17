
import typing

import sqlalchemy.sql
import sqlmodel
import sqlmodel.sql.expression


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

    @classmethod
    def fetch_feeds(cls, columns: typing.List[str] = None) -> typing.Union[sqlmodel.sql.expression.Select,
                                                                           sqlmodel.sql.expression.SelectOfScalar]:
        """
        Return SQL expression to fetch all feeds.

        :param columns:
        :return: sql expression
        """
        if columns:
            specific_cols = [sqlalchemy.sql.column(col) for col in columns]
            statement = sqlmodel.select(
                from_obj=Feed,
                columns=specific_cols,
            ) # noqa
        else:
            statement = sqlmodel.select(Feed)
        return statement

    @classmethod
    def fetch_single_feed(cls, feed_id: int) -> typing.Union[sqlmodel.sql.expression.Select,
                                                             sqlmodel.sql.expression.SelectOfScalar]:
        """
        Return SQL expression to fetch a single feed.

        :param feed_id: id of the feed
        :return: sql expression
        """
        return sqlmodel.select(Feed).where(Feed.id == feed_id)


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

    @classmethod
    def fetch_feeditems_for_feed(cls,
                                 feed_id: int,
                                 columns: typing.List[str] = None) \
            -> typing.Union[sqlmodel.sql.expression.Select, sqlmodel.sql.expression.SelectOfScalar]:
        """
        Return SQL expression to fetch all items of a feed.

        :param feed_id: id of the feed
        :param columns:
        :return: sql expression
        """
        if columns:
            specific_cols = [sqlalchemy.sql.column(col) for col in columns]
            statement = sqlmodel.select(
                from_obj=FeedItem,
                columns=specific_cols,
            ).where(FeedItem.feed == feed_id)  # noqa
        else:
            statement = sqlmodel.select(Feed).where(FeedItem.feed == feed_id)
        return statement

    @classmethod
    def delete_feed_item(cls, feed_item_id: int):
        """

        :param feed_item_id:
        :return:
        """
        return sqlmodel.delete(FeedItem).where(FeedItem.id == feed_item_id)

    @classmethod
    def delete_feed_items(cls, feed_item_ids: typing.List[int]):
        """

        :param feed_item_ids:
        :return:
        """
        return sqlmodel.delete(FeedItem).where(FeedItem.id.in_(feed_item_ids))


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
    playback_completion_date: int
    feeditem: int = sqlmodel.Field(default=None, foreign_key="FeedItems.id")
    played_duration: int
    last_played_time: int

    @classmethod
    def fetch_media_for_feed(cls,
                             feed_id: int,
                             columns: typing.List[str] = None) -> typing.Union[sqlmodel.sql.expression.Select,
                                                                               sqlmodel.sql.expression.SelectOfScalar]:
        """
        Return SQL expression to fetch all media of a feed.

        :param feed_id: id of the feed
        :param columns:
        :return: sql expression
        """
        if columns:
            specific_cols = [sqlalchemy.sql.column(col) if col != 'id' else FeedMedia.id
                             for col in columns]
            statement = sqlmodel.select(
                from_obj=FeedMedia,
                columns=specific_cols,
            ).filter(FeedMedia.feeditem == FeedItem.id).where(FeedItem.feed == feed_id)  # noqa
        else:
            statement = sqlmodel.select(FeedMedia) \
                .filter(FeedMedia.feeditem == FeedItem.id) \
                .where(FeedItem.feed == feed_id)
        return statement
