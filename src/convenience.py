import pandas as pd

import src.db_operations
import src.rssfeed.feed


def get_episodes_df_feed(sqlite_filename: str,
                         feed_id: int) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    feed = src.db_operations.fetch_from_db(kind='feeds',
                                           sqlite_filename=sqlite_filename,
                                           orm_model='sqlalchemy',
                                           where_cond={'id': feed_id})
    xml = src.rssfeed.feed.download_xml(url=feed.download_url.tolist()[0])
    if xml:
        return src.rssfeed.feed.parse_xml_for_episodes_df(xml=xml)
    return pd.DataFrame()
