import typing
import unittest

import pytest
import sqlalchemy
import sqlalchemy.orm

import src.orm.sqlalchemy.models

import tests.testdata


class TestingDatabase(unittest.TestCase):

    engine: typing.ClassVar[sqlalchemy.engine.Engine] = None
    session: typing.ClassVar[sqlalchemy.orm.Session] = None

    @classmethod
    def setUpClass(cls) -> None:

        # create engine and session
        cls.engine = sqlalchemy.engine.create_engine('sqlite+pysqlite:///:memory:')
        cls.session = sqlalchemy.orm.Session(autocommit=False, autoflush=False, bind=cls.engine, future=True)

        # create tables
        src.orm.sqlalchemy.models.Base.metadata.create_all(bind=cls.engine)

        # create entries
        cls.session.execute(sqlalchemy.insert(src.orm.sqlalchemy.models.Feed).values(tests.testdata.feeds))

    @classmethod
    def tearDownClass(cls) -> None:
        cls.session.close()
        cls.engine.dispose()

    def setUp(self) -> None:
        self.monkeypatch = pytest.MonkeyPatch()
