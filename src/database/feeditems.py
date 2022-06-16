import typing

import pandas as pd

import src.orm.peewee.fetch
import src.orm.pypika.fetch
import src.orm.sqlalchemy.fetch
import src.orm.sqlalchemy.update
import src.orm.sqlmodel.fetch


def get_feeditems_standard_columns() -> typing.List[str]:
    """

    :return:
    """
    return ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']


def create_fetch_feeditems_statement(orm_model: str,
                                     feed_id: int,
                                     columns: typing.Optional[typing.List[str]] = None) \
        -> typing.Tuple[typing.Optional[typing.Any], typing.List[str]]:
    """


    :param orm_model:
    :param feed_id:
    :param columns:
    :return:
    """
    columns_ = columns if columns else get_feeditems_standard_columns()
    if orm_model == 'sqlalchemy':
        statement = src.orm.sqlalchemy.fetch.create_fetch_feeditems_statement(columns=columns_,
                                                                              where_cond={'feed': feed_id})
    elif orm_model == 'sqlmodel':
        statement = src.orm.sqlmodel.fetch.create_fetch_feeditems_statement(columns=columns_,
                                                                            feed_id=feed_id)
    elif orm_model == 'pypika':
        statement = src.orm.pypika.fetch.create_fetch_feeditems_statement(columns=columns_,
                                                                          feed_id=feed_id)
    elif orm_model == 'peewee':
        statement = src.orm.peewee.fetch.create_fetch_feeditems_statement(columns=columns_,
                                                                          feed_id=feed_id)
    else:
        statement = None

    return statement, columns_


def create_update_feeditems_statements(orm_model: str,
                                       data: pd.DataFrame,
                                       columns: typing.List[str]):
    """

    :param orm_model:
    :param data:
    :param columns:
    :return:
    """
    if orm_model == 'sqlalchemy':
        statements = src.orm.sqlalchemy.update.create_update_feeditems_statements(data=data,
                                                                                  columns=columns)
    return statements
