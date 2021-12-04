
import sqlalchemy


def get_engine(sqlite_filename) -> sqlalchemy.engine.Engine:
    """

    :param sqlite_filename:
    :return:
    """
    return sqlalchemy.create_engine(f'sqlite+pysqlite:///{sqlite_filename}')


def get_connection(sqlite_filename: str) -> sqlalchemy.engine.Connection:
    """

    :param sqlite_filename:
    :return:
    """
    return get_engine(sqlite_filename=sqlite_filename).connect()
