import typing

import pandas as pd
import sqlalchemy

import src.orm.sqlalchemy.models


def create_update_feeditems_statements(data: pd.DataFrame,
                                       columns: typing.List[str],
                                       ) -> typing.List[sqlalchemy.sql.Update]:
    """
    Create statement to fetch all feeditems for a feed.

    :param data:
    :param columns:
    :return:
    """
    statements = []
    for ind, row in data.iterrows():
        statement = sqlalchemy.update(src.orm.sqlalchemy.models.FeedItem)
        statement = statement.where(src.orm.sqlalchemy.models.FeedItem.id == row.id)
        statement = statement.values({col: row[col] for col in columns})

        statements.append(statement)
    return statements
