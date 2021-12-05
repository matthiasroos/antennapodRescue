
import typing

import src.orm.peewee.fetch
import src.orm.sqlalchemy.fetch
import src.orm.sqlmodel.fetch


def create_fetch_feeds_statement(orm_model: str,
                                 columns: typing.Optional[typing.List[str]] = None):
    """


    :param orm_model:
    :param columns:
    :return:
    """
    columns_ = columns if columns else ['id', 'title', 'file_url', 'download_url', 'downloaded']
    if orm_model == 'sqlalchemy':
        statement = src.orm.sqlalchemy.fetch.create_fetch_feeds_statement(columns=columns_)
    elif orm_model == 'sqlmodel':
        statement = src.orm.sqlmodel.fetch.create_fetch_feeds_statement(columns=columns_)
    elif orm_model == 'peewee':
        statement = src.orm.peewee.fetch.create_fetch_feeds_statement(columns=columns_)

    return statement, columns_
