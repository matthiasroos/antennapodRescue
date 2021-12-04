import typing

import sqlmodel

import src.orm.sqlmodel.fetch
import src.orm.sqlmodel.models


def delete_feeditems_from_db(sqlite_filename: str, feed_item_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param feed_item_ids:
    :return:
    """

    engine = src.orm.sqlmodel.fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        statement = src.orm.sqlmodel.models.FeedItem.delete_feed_items(feed_item_ids=feed_item_ids)
        session.exec(statement)
        session.commit()


def delete_media_from_db(sqlite_filename: str, media_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param media_ids:
    :return:
    """

    engine = src.orm.sqlmodel.fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.delete(src.orm.sqlmodel.models.FeedMedia).where(
            src.orm.sqlmodel.models.FeedMedia.id.in_(media_ids))
        session.exec(statement)
        session.commit()
