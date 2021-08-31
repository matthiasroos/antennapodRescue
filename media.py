import typing

import pandas as pd
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
        statement = sqlmodel.select(models.FeedMedia)\
            .filter(models.FeedMedia.feeditem == models.FeedItem.id).where(models.FeedItem.feed == feed_id)
        media = session.exec(statement).all()
    return media


def get_media_df_from_db(sqlite_filename: str, feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    con = engine.connect()
    statement = sqlmodel.select(models.FeedMedia) \
        .filter(models.FeedMedia.feeditem == models.FeedItem.id).where(models.FeedItem.feed == feed_id)
    media_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=['id', 'duration', 'download_url', 'downloaded', 'filesize', 'feeditem'])
    con.close()
    return media_df
