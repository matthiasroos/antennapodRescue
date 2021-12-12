import peewee


def get_connection(sqlite_filename: str):
    """

    :param sqlite_filename:
    :return:
    """
    return peewee.SqliteDatabase(sqlite_filename).connection()
