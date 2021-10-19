import typing

import sqlmodel

import sqlmodel_.database.fetch
import sqlmodel_.models


def delete_feeditems_from_db(sqlite_filename: str, feed_item_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param feed_item_ids:
    :return:
    """

    engine = sqlmodel_.database.fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel_.models.FeedItem.delete_feed_items(feed_item_ids=feed_item_ids)
        session.exec(statement)
        session.commit()


def delete_media_from_db(sqlite_filename: str, media_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param media_ids:
    :return:
    """

    engine = sqlmodel_.database.fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.delete(sqlmodel_.models.FeedMedia).where(sqlmodel_.models.FeedMedia.id.in_(media_ids))
        session.exec(statement)
        session.commit()
