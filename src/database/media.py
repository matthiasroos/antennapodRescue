import dataclasses
import typing

import model
import src.orm.peewee.fetch
import src.orm.pypika.fetch
import src.orm.sqlalchemy.fetch
import src.orm.sqlmodel.fetch


def get_media_standard_columns() -> typing.List[str]:
    """

    :return:
    """
    return [field.name for field in dataclasses.fields(model.FeedMedia)]


def create_fetch_media_statement(orm_model: str,
                                 feed_id: int,
                                 columns: typing.Optional[typing.List[str]] = None) \
        -> typing.Tuple[typing.Optional[typing.Any], typing.List[str]]:
    """


    :param orm_model:
    :param feed_id:
    :param columns:
    :return:
    """
    columns_ = columns if columns else get_media_standard_columns()
    if orm_model == 'sqlalchemy':
        statement = src.orm.sqlalchemy.fetch.create_fetch_media_statement(columns=columns_,
                                                                          feed_id=feed_id)
    elif orm_model == 'sqlmodel':
        statement = src.orm.sqlmodel.fetch.create_fetch_media_statement(columns=columns_,
                                                                        feed_id=feed_id)
    elif orm_model == 'peewee':
        statement = src.orm.peewee.fetch.create_fetch_media_statement(columns=columns_,
                                                                      feed_id=feed_id)
    else:
        statement = None

    return statement, columns_
