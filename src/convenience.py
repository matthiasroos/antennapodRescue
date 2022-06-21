import asyncio
import typing

import pandas as pd
import sqlalchemy.orm

import src.db_operations
import src.orm.sqlalchemy.database
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


async def get_data_for_feed(sqlite_filename: str,
                            feed_id: int) -> typing.Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """

    :param sqlite_filename:
    :param feed_id:
    :return:
    """
    statements = []
    columns = []
    for kind, col_name in zip(['feeds', 'feeditems', 'media'], ['id', 'feed', 'feed_id']):
        statement, columns_ = src.db_operations.create_fetch_statement_for_kind(orm_model='sqlalchemy',
                                                                                kind=kind,
                                                                                columns=[],
                                                                                where_cond={col_name: feed_id},
                                                                                )
        statements.append(statement)
        columns.append(columns_)

    results = await src.orm.sqlalchemy.database.execute_statements_async(sqlite_filename=sqlite_filename,
                                                                         statements=statements)

    dfs = [pd.DataFrame.from_records(res, columns=col) for res, col in zip(results, columns)]

    return dfs
