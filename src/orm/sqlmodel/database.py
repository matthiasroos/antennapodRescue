import typing

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


def execute_statements(sqlite_filename: str,
                       statements: typing.List):
    """

    :param sqlite_filename:
    :param statements:
    :return:
    """
    connection = get_connection(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(connection) as session:
        results = []
        for statement in statements:
            result = session.exec(statement)
            results.append(result)
    return results
