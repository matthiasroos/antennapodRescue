
import typing

import pandas as pd
import peewee
import pypika
import sqlalchemy.engine
import sqlmodel.sql.expression


def fetch_all_df(connection: typing.Union[
                    sqlalchemy.engine.Connection,
                    peewee.Database],
                 statement: typing.Union[
                     sqlalchemy.sql.selectable.Select,
                     typing.Union[sqlmodel.sql.expression.Select, sqlmodel.sql.expression.SelectOfScalar],
                     peewee.ModelSelect
                 ],
                 columns: typing.List[str]) -> pd.DataFrame:
    """
    Base method.
    Fetch all rows of a table from db and return them as a dataframe.

    :param connection: file name of the sqlite database file
    :param statement: SQL query to be executed
    :param columns: list of column names
    :return: dataframe containing all data
    """
    if isinstance(statement, peewee.ModelSelect):
        sql, params = statement.sql()
    elif isinstance(statement, pypika.queries.QueryBuilder):
        sql = statement.get_sql()
        params = None
    else:
        sql = statement
        params = None

    data_df = pd.read_sql(
        sql=sql,
        params=params,
        con=connection,
        columns=columns)
    connection.close()
    return data_df
