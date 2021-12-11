import typing

import sqlalchemy
import sqlalchemy.orm


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


def execute_statements(sqlite_filename: str,
                       statements: typing.List):
    """

    :param sqlite_filename:
    :param statements:
    :return:
    """
    connection = get_connection(sqlite_filename=sqlite_filename)
    with sqlalchemy.orm.Session(connection) as session:
        for statement in statements:
            session.execute(statement)
        session.commit()
