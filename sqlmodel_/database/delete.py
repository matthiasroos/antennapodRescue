import typing

import sqlmodel

import fetch
import sqlmodel_.models


def delete_feeditems_from_db(sqlite_filename: str, feed_item_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param feed_item_ids:
    :return:
    """

    engine = fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        for feed_item_id in feed_item_ids:
            statement = sqlmodel_.models.FeedItem.delete_feed_item(feed_item_id=feed_item_id)
            session.exec(statement)
        session.commit()


def delete_media_from_db(sqlite_filename: str, media_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param media_ids:
    :return:
    """

    engine = fetch.get_engine(sqlite_filename=sqlite_filename)
    with sqlmodel.Session(engine) as session:
        for media_id in media_ids:
            statement = sqlmodel.delete(sqlmodel_.models.FeedMedia).where(sqlmodel_.models.FeedMedia.id == media_id)
            session.exec(statement)
        session.commit()
