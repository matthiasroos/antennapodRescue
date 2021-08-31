import typing

import pandas as pd
import sqlmodel


def fetch_all(sqlite_filename: str, statement):
    """

    :param sqlite_filename:
    :param statement:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    with sqlmodel.Session(engine) as session:
        data = session.exec(statement).all()
    return data


def fetch_all_df(sqlite_filename: str, statement, columns: typing.List[str]) -> pd.DataFrame:
    """

    :param sqlite_filename:
    :param statement:
    :param columns:
    :return:
    """
    engine = sqlmodel.create_engine(f'sqlite:///{sqlite_filename}')
    con = engine.connect()
    data_df = pd.read_sql(
        sql=statement,
        con=con,
        columns=columns)
    con.close()
    return data_df
