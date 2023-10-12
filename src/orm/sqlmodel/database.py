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
                       connection: sqlalchemy.engine.Engine,
                       statements: typing.List):
    """

    :param sqlite_filename:
    :param connection:
    :param statements:
    :return:
    """
    if sqlite_filename is None and connection is None:
        return None, None
    if sqlite_filename:
        connection = get_connection(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(connection) as session:
        results = []
        columns = []
        for statement in statements:
            result = session.exec(statement)
            results.append(result)
            columns.append(result.keys())
        session.commit()
    return results, columns
