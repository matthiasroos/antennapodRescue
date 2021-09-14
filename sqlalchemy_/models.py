import datetime
import typing

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.types


Base = sqlalchemy.orm.declarative_base()


class EpochTimestamp(sqlalchemy.types.TypeDecorator):
    """
    Type extension for Integer as Epoch
    """
    impl = sqlalchemy.types.Integer

    def process_bind_param(self, value, dialect):
        """
        Manipulate input parameter before writing it to the database.
        """
        return value.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000

    def process_result_value(self, value, dialect):
        """
        Convert value coming as result from the db.
        """
        return datetime.datetime.utcfromtimestamp(value / 1000)


class Milliseconds(sqlalchemy.types.TypeDecorator):
    """
    Type extension for Integer as Milliseconds
    """
    impl = sqlalchemy.types.Integer

    def process_bind_param(self, value, dialect):
        """
        Manipulate input parameter before writing it to the database.
        """
        return value.milliseconds

    def process_result_value(self, value, dialect):
        """
        Convert value coming as result from the db.
        """
        return datetime.timedelta(milliseconds=value)


class Feed(Base):
    """
    Table Feeds
    """
    __tablename__ = 'Feeds'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    title = sqlalchemy.Column(sqlalchemy.String)
    file_url = sqlalchemy.Column(sqlalchemy.String)
    download_url = sqlalchemy.Column(sqlalchemy.String)
    downloaded = sqlalchemy.Column(sqlalchemy.Integer)


class FeedItem(Base):
    """
    Table FeedItems
    """
    __tablename__ = 'FeedItems'

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    title = sqlalchemy.Column(sqlalchemy.String)
    pubDate = sqlalchemy.Column(EpochTimestamp)
    read = sqlalchemy.Column(sqlalchemy.Integer)
    link = sqlalchemy.Column(sqlalchemy.String)
    description = sqlalchemy.Column(sqlalchemy.String)

    feed = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('Feeds.id'))

    item_identifier = sqlalchemy.Column(sqlalchemy.String)
    image_url = sqlalchemy.Column(sqlalchemy.String)


class FeedMedia(Base):
    """
    Table FeedMedia
    """
    __tablename__ = "FeedMedia"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    duration: datetime.timedelta = sqlalchemy.Column(Milliseconds)
    download_url = sqlalchemy.Column(sqlalchemy.String)
    downloaded = sqlalchemy.Column(sqlalchemy.Integer)
    filesize = sqlalchemy.Column(sqlalchemy.Integer)
    playback_completion_date: int = sqlalchemy.Column(sqlalchemy.Integer)

    feeditem = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('FeedItems.id'))

    played_duration: int = sqlalchemy.Column(sqlalchemy.Integer)
    last_played_time: int = sqlalchemy.Column(sqlalchemy.Integer)
