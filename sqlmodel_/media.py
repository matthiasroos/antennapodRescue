import typing

import pandas as pd
import sqlmodel

import sqlmodel_.database
import sqlmodel_.models


def get_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[sqlmodel_.models.FeedMedia]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedMedia) \
        .filter(sqlmodel_.models.FeedMedia.feeditem == sqlmodel_.models.FeedItem.id) \
        .where(sqlmodel_.models.FeedItem.feed == feed_id)
    media = sqlmodel_.database.fetch_all(sqlite_filename=sqlite_filename,
                                         statement=statement)
    return media


def get_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statement = sqlmodel.select(sqlmodel_.models.FeedMedia) \
        .filter(sqlmodel_.models.FeedMedia.feeditem == sqlmodel_.models.FeedItem.id)\
        .where(sqlmodel_.models.FeedItem.feed == feed_id)
    columns = ['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem']
    media_df = sqlmodel_.database.fetch_all_df(sqlite_filename=sqlite_filename,
                                               statement=statement,
                                               columns=columns)
    return media_df


def delete_media_from_db(sqlite_filename: str, media_ids: typing.List[int]) -> None:
    """

    :param sqlite_filename:
    :param media_ids:
    :return:
    """

    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        for media_id in media_ids:
            statement = sqlmodel.delete(sqlmodel_.models.FeedMedia).where(sqlmodel_.models.FeedMedia.id == media_id)
            session.exec(statement)
        session.commit()
