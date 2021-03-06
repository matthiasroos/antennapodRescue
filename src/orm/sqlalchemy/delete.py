import typing

import sqlalchemy.orm

import fetch
import src.orm.sqlalchemy.models


def execute_delete_statement(sqlite_filename: str, statement: sqlalchemy.sql.Delete):
    """

    :param sqlite_filename:
    :param statement:
    :return:
    """
    engine = fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlalchemy.orm.Session(engine) as session:
        session.execute(statement=statement)
        session.commit()


def delete_feeditems_from_db(sqlite_filename: str, feed_items_id: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param feed_items_id:
    :return:
    """
    statement = \
        sqlalchemy.sql.delete(src.orm.sqlalchemy.models.FeedItem).where(
            src.orm.sqlalchemy.models.FeedItem.id.in_(feed_items_id))
    execute_delete_statement(sqlite_filename=sqlite_filename,
                             statement=statement)


def delete_media_from_db(sqlite_filename: str, media_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param media_ids:
    :return:
    """
    statement = \
        sqlalchemy.sql.delete(src.orm.sqlalchemy.models.FeedMedia).where(
            src.orm.sqlalchemy.models.FeedMedia.id.in_(media_ids))
    execute_delete_statement(sqlite_filename=sqlite_filename,
                             statement=statement)
