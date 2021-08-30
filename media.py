import typing

import sqlmodel

import episodes


class FeedMedia(sqlmodel.SQLModel, table=True):
    """
    Table FeedMedia
    """
    id: typing.Optional[int] = sqlmodel.Field(default=None, primary_key=True)
    duration: int
    download_url: str
    downloaded: int
    filesize: int
    feeditem: int


def get_media_from_db(sqlite_filename: str, feed_id: int) -> typing.List[FeedMedia]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        statement = sqlmodel.select(FeedMedia)
        media = session.exec(statement).all()
    return media
