
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
