import src.orm.aiosql.database
import src.orm.peewee.database
import src.orm.pony.database
import src.orm.sqlalchemy.database
import src.orm.sqlmodel.database


def get_connection(orm_model: str,
                   sqlite_filename: str):
    """

    :param orm_model:
    :param sqlite_filename:
    :return:
    """

    if orm_model == 'sqlalchemy':
        connection = src.orm.sqlalchemy.database.get_async_connection(sqlite_filename=sqlite_filename)
    elif orm_model == 'sqlmodel':
        connection = src.orm.sqlmodel.database.get_connection(sqlite_filename=sqlite_filename)
    elif orm_model == 'peewee':
        connection = src.orm.peewee.database.get_connection(sqlite_filename=sqlite_filename)
    elif orm_model == 'aiosql':
        connection = src.orm.aiosql.database.get_connection(file_name=sqlite_filename)
    elif orm_model == 'pony':
        connection = src.orm.pony.database.get_connection(file_name=sqlite_filename)
    else:
        connection = src.orm.sqlalchemy.database.get_connection(sqlite_filename=sqlite_filename)

    return connection
