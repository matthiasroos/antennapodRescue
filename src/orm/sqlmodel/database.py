import sqlalchemy.engine
import sqlmodel


def get_engine(sqlite_filename: str) -> sqlalchemy.engine.Engine:
    """

    :param sqlite_filename:
    :return:
    """
    return sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')


def get_connection(sqlite_filename: str) -> sqlalchemy.engine.Connection:
    """

    :param sqlite_filename:
    :return:
    """
    return get_engine(sqlite_filename=sqlite_filename).connect()
