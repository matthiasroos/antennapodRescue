import typing

import sqlalchemy
import sqlalchemy.ext.asyncio
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
    with sqlalchemy.orm.Session(connection) as session:
        results = []
        columns = []
        for statement in statements:
            result = session.execute(statement)
            results.append(result)
            columns.append(result.keys())
        session.commit()
    return results, columns


def get_async_engine(sqlite_filename: str) -> sqlalchemy.ext.asyncio.AsyncEngine:
    """

    :param sqlite_filename:
    :return:
    """
    return sqlalchemy.ext.asyncio.create_async_engine(f'sqlite+aiosqlite:///{sqlite_filename}')


def get_async_connection(sqlite_filename: str) -> sqlalchemy.ext.asyncio.AsyncConnection:
    """

    :param sqlite_filename:
    :return:
    """
    return get_async_engine(sqlite_filename=sqlite_filename).connect()


async def execute_statements_async(sqlite_filename: str,
                                   statements: typing.List):
    """

    :param sqlite_filename:
    :param statements:
    :return:
    """
    engine = get_async_engine(sqlite_filename=sqlite_filename)
    async with sqlalchemy.ext.asyncio.AsyncSession(bind=engine) as session:
        results = []
        for statement in statements:
            result = await session.execute(statement)
            results.append(result)

    await engine.dispose()
    return results
