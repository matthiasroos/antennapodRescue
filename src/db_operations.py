import typing

import pandas as pd

import src.orm.sqlalchemy.database
import src.database.connection
import src.database.fetch
import src.database.feeditems
import src.database.feeds
import src.database.media
import src.database.update


def fetch_from_db(kind: str,
                  sqlite_filename: str,
                  orm_model: str = 'sqlalchemy',
                  columns: typing.Optional[typing.List[str]] = None,
                  sort_by: typing.Iterable[str] = None,
                  options: typing.Dict[str, typing.Any] = None,
                  ) -> pd.DataFrame:
    """

    :param kind: kind of elements to fetch from db, possible values: 'feeds', 'feeditems', 'media'
    :param sqlite_filename: file name of the sqlite database file
    :param orm_model: ORM model to be used , default: 'sql_alchemy'; other possibles values: ...
    :param columns:
    :param sort_by: list of column names to sorted by
    :param options:
    :return:
    """
    sort_by = [] if sort_by is None else list(sort_by)

    connection = src.database.connection.get_connection(orm_model=orm_model,
                                                        sqlite_filename=sqlite_filename)

    if kind == 'feeds':
        statement, columns_ = src.database.feeds.create_fetch_feeds_statement(orm_model=orm_model,
                                                                              columns=columns)
    elif kind == 'feeditems':
        statement, columns_ = src.database.feeditems.create_fetch_feeditems_statement(orm_model=orm_model,
                                                                                      feed_id=options['feed_id'],
                                                                                      columns=columns)
    elif kind == 'media':
        statement, columns_ = src.database.media.create_fetch_media_statement(orm_model=orm_model,
                                                                              feed_id=options['feed_id'],
                                                                              columns=columns)
    else:
        statement, columns_ = ...

    elements_df = src.database.fetch.fetch_all_df(connection=connection,
                                                  statement=statement,
                                                  columns=columns_)

    elements_df = elements_df.sort_values(by=sort_by)
    return elements_df


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
        statements = src.database.feeditems.create_update_feeditems_statements(orm_model=orm_model,
                                                                               data=data,
                                                                               columns=columns)
    else:
        statements = ...

    src.database.update.update_db(orm_model=orm_model,
                                  sqlite_filename=sqlite_filename,
                                  statements=statements)
