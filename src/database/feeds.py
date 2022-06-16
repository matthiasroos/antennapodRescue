
import typing

import src.orm.peewee.fetch
import src.orm.pypika.fetch
import src.orm.sqlalchemy.fetch
import src.orm.sqlmodel.fetch


def get_feed_standard_columns() -> typing.List[str]:
    """

    :return:
    """
    return ['id', 'title', 'file_url', 'download_url', 'downloaded']


def create_fetch_feeds_statement(orm_model: str,
                                 columns: typing.Optional[typing.List[str]] = None,
                                 where_cond: typing.Dict[str, typing.Any] = None) \
        -> typing.Tuple[typing.Optional[typing.Any], typing.List[str]]:
    """


    :param orm_model:
    :param columns:
    :param where_cond:
    :return:
    """
    columns_ = columns if columns else get_feed_standard_columns()
    if orm_model == 'sqlalchemy':
        statement = src.orm.sqlalchemy.fetch.create_fetch_feeds_statement(columns=columns_,
                                                                          where_cond=where_cond)
    elif orm_model == 'sqlmodel':
        statement = src.orm.sqlmodel.fetch.create_fetch_feeds_statement(columns=columns_,
                                                                        where_cond=where_cond)
    elif orm_model == 'pypika':
        statement = src.orm.pypika.fetch.create_fetch_feeds_statement(columns=columns_,
                                                                      where_cond=where_cond)
    elif orm_model == 'peewee':
        statement = src.orm.peewee.fetch.create_fetch_feeds_statement(columns=columns_,
                                                                      where_cond=where_cond)
    else:
        statement = None

    return statement, columns_
