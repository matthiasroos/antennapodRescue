import typing

import src.orm.peewee.fetch
import src.orm.sqlalchemy.fetch
import src.orm.sqlmodel.fetch


def create_fetch_feeditems_statement(orm_model: str,
                                     feed_id: int,
                                     columns: typing.Optional[typing.List[str]] = None):
    """


    :param orm_model:
    :param feed_id:
    :param columns:
    :return:
    """
    columns_ = columns if columns else \
        ['id', 'title', 'pubDate', 'read', 'description', 'link', 'feed', 'item_identifier', 'image_url']
    if orm_model == 'sqlalchemy':
        statement = src.orm.sqlalchemy.fetch.create_fetch_feeditems_statement(columns=columns_,
                                                                              feed_id=feed_id)

    return statement, columns_
