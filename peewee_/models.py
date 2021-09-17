
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
