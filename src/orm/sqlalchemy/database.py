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
