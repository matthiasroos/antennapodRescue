import datetime

import pony.orm
import pony.orm.dbapiprovider


db = pony.orm.Database()


class Epoch:
    """
    Epoch class
    """
    pass


class EpochConverter(pony.orm.dbapiprovider.IntConverter):
    """
    Converter for Integer as Epoch
    """

    def validate(self, val):
        """

        :param val:
        :return:
        """
        return val

    def py2sql(self, val):
        """
        Manipulate input parameter before writing it to the database.
        """
        return val.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000

    def sql2py(self, value):
        """
        Convert value coming as a result from the db.
        """
        return datetime.datetime.utcfromtimestamp(value / 1000)


class Feed(db.Entity):
    """
    Table Feeds
    """
    _table_ = 'Feeds'

    id = pony.orm.PrimaryKey(int)
    title = pony.orm.Required(str)
    file_url = pony.orm.Required(str)
    download_url = pony.orm.Required(str)
    downloaded = pony.orm.Required(str)


class FeedItem(db.Entity):
    """
    Table FeedItems
    """
    _table_ = 'FeedItems'

    id = pony.orm.PrimaryKey(int)
    title = pony.orm.Required(str)
    pubDate = pony.orm.Required(Epoch)
    read = pony.orm.Required(int)
    link = pony.orm.Required(str)
    description = pony.orm.Required(str)

    feed = pony.orm.Required(int)

    item_identifier = pony.orm.Required(str)
    image_url = pony.orm.Optional(str)
