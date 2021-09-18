
import peewee


class Feed(peewee.Model):
    """
    Table Feeds
    """

    id = peewee.IntegerField(unique=True, primary_key=True)
    title = peewee.CharField()
    file_url = peewee.CharField()
    download_url = peewee.CharField()
    downloaded = peewee.IntegerField()

    class Meta:
        """Inner class"""
        table_name = 'Feeds'


class FeedItem(peewee.Model):
    """
    Table FeedItems
    """
    id = peewee.IntegerField(unique=True, primary_key=True)
    title = peewee.CharField()
    pubDate = peewee.IntegerField()
    read = peewee.IntegerField()
    link = peewee.CharField()
    description = peewee.CharField()

    feed = peewee.IntegerField()

    item_identifier = peewee.CharField()
    image_url = peewee.CharField()

    class Meta:
        """Meta Class"""
        table_name = 'FeedItems'


class FeedMedia(peewee.Model):
    """
    Table FeedMedia
    """
    id = peewee.IntegerField(unique=True, primary_key=True)
    duration = peewee.IntegerField()
    download_url = peewee.IntegerField()
    downloaded = peewee.CharField()
    filesize = peewee.IntegerField()
    playback_completion_date = peewee.IntegerField()

    feeditem = peewee.IntegerField()

    played_duration = peewee.IntegerField()
    last_played_time = peewee.IntegerField()

    class Meta:
        """Meta Class"""
        table_name = 'FeedMedia'
