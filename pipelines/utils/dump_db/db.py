# -*- coding: utf-8 -*-
"""
Database definitions for SQL pipelines.
"""

from abc import ABC, abstractmethod
from typing import List

import cx_Oracle
import pymongo
import pymysql.cursors
import pyodbc
from pipelines.utils.utils import log


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
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self._hostname},{self._port};"
            f"DATABASE={self._database};"
            f"UID={self._user};"
            f"PWD={self._password};"
            "Encrypt=no;"
            "TrustServerCertificate=yes;"
        )
        return pyodbc.connect(conn_str)

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
        port = port if isinstance(port, int) else int(port)
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


class MongoDB(Database):
    """
    MongoDB database.
    Compatible with MongoDB 3.4+
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        port: int = 27017,
    ) -> None:
        """
        Initializes the MongoDB database.

        Args:
            hostname: The hostname of the database.
            port: The port of the database.
            user: The username of the database.
            password: The password of the database.
            database: The database name.
        """
        port = port if isinstance(port, int) else int(port)
        self._collection_name = None
        self._documents = []
        self._current_index = 0
        self._columns = []
        super().__init__(
            hostname,
            port,
            user,
            password,
            database,
        )

    def connect(self):
        """
        Connect to the MongoDB.
        """
        connection_string = (
            f"mongodb://{self._user}:{self._password}@"
            f"{self._hostname}:{self._port}/{self._database}"
        )
        log(f"Connecting to MongoDB: {self._hostname}:{self._port}/{self._database}")
        client = pymongo.MongoClient(connection_string)
        return client[self._database]

    def get_cursor(self):
        """
        Returns a cursor for the MongoDB.
        For MongoDB, we return the database object itself as we'll use it directly.
        """
        return self._connection

    def execute_query(self, query: str) -> None:
        """
        Execute query on the MongoDB.
        For MongoDB, the 'query' parameter should be the collection name.
        Example: 'files' or 'files.chunks'

        Args:
            query: The collection name to query.
        """
        # Extract collection name from query
        # If query is SQL-like (e.g., "SELECT * FROM collection_name"),
        # extract the collection name
        if "FROM" in query.upper():
            parts = query.upper().split("FROM")
            if len(parts) > 1:
                collection_name = parts[1].strip().split()[0].strip()
            else:
                collection_name = query.strip()
        else:
            collection_name = query.strip()

        log(f"Querying MongoDB collection: {collection_name}")
        self._collection_name = collection_name

        # Get the collection
        collection = self._cursor[collection_name]

        # Fetch all documents from the collection
        self._documents = list(collection.find())

        log(f"Found {len(self._documents)} documents in collection {collection_name}")

        # Extract column names from the first document
        if self._documents:
            # Get all unique keys from all documents (MongoDB documents can have different schemas)
            all_keys = set()
            for doc in self._documents:
                all_keys.update(doc.keys())
            self._columns = sorted(list(all_keys))
        else:
            self._columns = []

        # Reset index for fetching
        self._current_index = 0

    def get_columns(self) -> List[str]:
        """
        Returns the column names of the MongoDB collection.
        """
        return self._columns

    def fetch_batch(self, batch_size: int) -> List[List]:
        """
        Fetches a batch of documents from the MongoDB collection.
        Converts documents to lists based on column order.
        """
        batch = []
        end_index = min(self._current_index + batch_size, len(self._documents))

        for i in range(self._current_index, end_index):
            doc = self._documents[i]
            # Convert document to list based on column order
            row = []
            for col in self._columns:
                value = doc.get(col)
                # Convert ObjectId to string for compatibility
                if isinstance(value, pymongo.objectid.ObjectId):
                    value = str(value)
                # Convert Binary data to string representation
                elif isinstance(value, bytes):
                    value = value.hex()
                row.append(value)
            batch.append(row)

        self._current_index = end_index
        return batch

    def fetch_all(self) -> List[List]:
        """
        Fetches all remaining documents from the MongoDB collection.
        """
        return self.fetch_batch(len(self._documents) - self._current_index)
