
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
