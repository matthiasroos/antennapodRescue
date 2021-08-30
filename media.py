import typing

import sqlmodel

import models


def get_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[models.FeedMedia]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(models.FeedMedia)
        media = session.exec(statement).all()
    return media
