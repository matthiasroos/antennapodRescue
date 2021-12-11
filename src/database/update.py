import typing

import src.orm.sqlalchemy.database


def update_db(orm_model: str,
              sqlite_filename: str,
              statements: typing.List):
    """

    :param orm_model:
    :param sqlite_filename:
    :param statements:
    :return:
    """
    if orm_model == 'sqlalchemy':
        src.orm.sqlalchemy.database.execute_statements(sqlite_filename=sqlite_filename,
                                                       statements=statements)
