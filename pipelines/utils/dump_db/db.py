# -*- coding: utf-8 -*-
"""
Database definitions for SQL pipelines.
"""

from abc import ABC, abstractmethod
from typing import List

import cx_Oracle
import pymssql
import pymysql.cursors


class Database(ABC):
    """
    Database abstract class.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        port: int,
        user: str,
        password: str,
        database: str,
    ) -> None:
        """
        Initializes the database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
        """
        self._hostname = hostname
        self._port = port
        self._user = user
        self._password = password
        self._database = database
        self._connection = self.connect()
        self._cursor = self.get_cursor()

    @abstractmethod
    def connect(self):
        """
        Connect to the database.
        """

    @abstractmethod
    def get_cursor(self):
        """
        Returns a cursor for the database.
        """

    @abstractmethod
    def execute_query(self, query: str) -> None:
        """
        Execute query on the database.

        Args:
            query: The query to execute.
        """

    @abstractmethod
    def get_columns(self) -> List[str]:
        """
        Returns the column names of the database.
        """

    @abstractmethod
    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the database.
        """

    @abstractmethod
    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the database.
        """


class SqlServer(Database):
    """
    SQL Server database.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 1433,
    ) -> None:
        """
        Initializes the SQL Server database.

        Args:
            hostname: The hostname of the SQL Server.
            port: The port of the SQL Server.
            user: The username of the SQL Server.
            password: The password of the SQL Server.
            database: The database name.
        """
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the SQL Server.
        """
        # pylint: disable=E1101
        return pymssql.connect(
            server=self._hostname,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

    def get_cursor(self):
        """
        Returns a cursor for the SQL Server.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the SQL Server.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the SQL Server.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the SQL Server.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the SQL Server.
        """
        return [list(item) for item in self._cursor.fetchall()]


class MySql(Database):
    """
    MySQL database.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 3306,
    ) -> None:
        """
        Initializes the MySQL database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
        """
        port = port if type(port) == int else int(port)
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the MySQL.
        """
        # pylint: disable=E1101
        return pymysql.connect(
            host=self._hostname,
            port=self._port,
            user=self._user,
            password=self._password,
            database=self._database,
        )

    def get_cursor(self):
        """
        Returns a cursor for the MySQL.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the MySQL.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the MySQL.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the MySQL.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the MySQL.
        """
        return [list(item) for item in self._cursor.fetchall()]


class Oracle(Database):
    """
    Oracle Database
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 1521,
    ) -> None:
        """
        Initializes the Oracle database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
        """
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the Oracle.
        """
        # pylint: disable=E1101
        return cx_Oracle.connect(
            f"{self._user}/{self._password}@"
            f"{self._hostname}:{self._port}/{self._database}"
        )

    def get_cursor(self):
        """
        Returns a cursor for the Oracle.
        """
        return self._connection.cursor()

    def execute_query(self, query: str) -> None:
        """
        Execute query on the Oracle.

        Args:
            query: The query to execute.
        """
        self._cursor.execute(query)

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the Oracle.
        """
        return [column[0] for column in self._cursor.description]

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of rows from the Oracle.
        """
        return [list(item) for item in self._cursor.fetchmany(batch_size)]

    def fetch_all(self) -> List[List]:
        """
        Fetches all rows from the Oracle.
        """
        return [list(item) for item in self._cursor.fetchall()]
