import asyncio
import dataclasses
import importlib
import typing

import pandas as pd

import model
import src.orm.aiosql.database
import src.orm.peewee.fetch
import src.orm.pony.database
import src.orm.pypika.fetch
import src.orm.sqlalchemy.database
import src.orm.sqlalchemy.fetch
import src.orm.sqlmodel.database
import src.orm.sqlmodel.fetch
import src.database.connection
import src.database.fetch
import src.database.update


def create_fetch_statement_for_kind(orm_model: str,
                                    kind: str,
                                    columns: typing.List[str],
                                    where_cond: typing.Dict[str, typing.Any] = None) \
     -> typing.Tuple[typing.Any,
                     typing.List[str]]:
    """

    :param orm_model:
    :param kind:
    :param columns:
    :param where_cond:
    :return:
    """
    if orm_model not in ['sqlalchemy', 'sqlmodel', 'pypika', 'peewee']:
        return None, []
    orm_module = importlib.import_module(f'src.orm.{orm_model}.fetch')
    if kind == 'feeds':
        columns_ = columns if columns else [field.name for field in dataclasses.fields(model.Feed)]
        statement = getattr(orm_module, 'create_fetch_feeds_statement')(columns=columns_, where_cond=where_cond)
    elif kind == 'feeditems':
        columns_ = columns if columns else [field.name for field in dataclasses.fields(model.FeedItem)]
        statement = getattr(orm_module, 'create_fetch_feeditems_statement')(columns=columns_, where_cond=where_cond)
    elif kind == 'media':
        columns_ = columns if columns else [field.name for field in dataclasses.fields(model.FeedMedia)]
        statement = getattr(orm_module, 'create_fetch_media_statement')(columns=columns_, where_cond=where_cond)
    else:
        statement, columns_ = None, []

    return statement, columns_


def fetch_from_db(kind: str,
                  sqlite_filename: str,
                  orm_model: str = 'sqlalchemy',
                  columns: typing.Optional[typing.List[str]] = None,
                  sort_by: typing.Iterable[str] = None,
                  where_cond: typing.Dict[str, typing.Any] = None,
                  ) -> typing.Optional[pd.DataFrame]:
    """

    :param kind: kind of elements to fetch from db, possible values: 'feeds', 'feeditems', 'media'
    :param sqlite_filename: file name of the sqlite database file
    :param orm_model: ORM model to be used , default: 'sql_alchemy'; other possibles values: ...
    :param columns:
    :param sort_by: list of column names to sorted by
    :param where_cond:
    :return:
    """
    sort_by = [] if sort_by is None else list(sort_by)

    connection = src.database.connection.get_connection(orm_model=orm_model,
                                                        sqlite_filename=sqlite_filename)

    elements_df = None

    if orm_model in ['sqlalchemy', 'sqlmodel', 'pypika', 'peewee']:
        statement, columns_ = create_fetch_statement_for_kind(orm_model=orm_model,
                                                              kind=kind,
                                                              columns=columns,
                                                              where_cond=where_cond)

    elif orm_model == 'aiosql':
        statement, columns_ = None, []
        elements_df = src.orm.aiosql.database.fetch_kind(kind=kind,
                                                         connection=connection,
                                                         where_cond=where_cond)
    elif orm_model == 'pony':
        statement, columns_ = None, []
        elements_df = src.orm.pony.database.fetch_kind(kind=kind,
                                                       connection=connection,
                                                       where_cond=where_cond)
    else:
        statement, columns_ = None, []

    if elements_df is None and statement is not None and columns_:
        elements_df = src.database.fetch.fetch_all_df(connection=connection,
                                                      statement=statement,
                                                      columns=columns_)


    elements_df = elements_df.sort_values(by=sort_by)
    return elements_df

def create_update_statements_for_kind(orm_model: str,
                                     kind: str,
                                     data: list[model.AntennaPodElement],
                                     columns: list[str]) -> typing.Optional[list[sqlalchemy.sql.Update]]:
    """

    :param orm_model:
    :param kind:
    :param data:
    :param columns:
    :return:
    """
    if orm_model not in ['sql_alchemy']:
        return None
    orm_module = importlib.import_module(f'src.orm.{orm_model}.update')
    if kind == 'feeditems':
        statements = getattr(orm_module, 'create_update_feeditems_statements')(data=data,
                                                                               columns=columns)

    return statements


def update_db(kind: str,
              sqlite_filename: str,
              columns: typing.List[str],
              data: pd.DataFrame,
              orm_model: str = 'sqlalchemy',
              options: typing.Dict[str, typing.Any] = None,
              ):
    """

    :param kind:
    :param sqlite_filename:
    :param orm_model:
    :param columns:
    :param data:
    :param options:
    :return:
    """

    if kind == 'feeditems':
        statements = create_update_statements_for_kind(orm_model=orm_model,
                                                       data=data,
                                                       columns=columns)
    else:
        statements = None

    if statements:

        src.database.update.update_db(orm_model=orm_model,
                                      sqlite_filename=sqlite_filename,
                                      statements=statements)
