import pony.orm


db = pony.orm.Database()


class Feed(db.Entity):
    """

    """
    _table_ = 'Feeds'

    id = pony.orm.PrimaryKey(int)
    title = pony.orm.Required(str)
    file_url = pony.orm.Required(str)
    download_url = pony.orm.Required(str)
    downloaded = pony.orm.Required(str)
