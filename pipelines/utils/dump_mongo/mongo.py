# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional

from pymongo import MongoClient


class Mongo:
    """
    MongoDB database.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        hostname: str,
        user: str,
        password: str,
        database: str,
        collection: str,
        port: Optional[int] = 27017,
    ) -> None:
        """
        Initializes the MongoDB database.

        Args:
            hostname (str): Hostname of the MongoDB.
            user (str): Username of the MongoDB.
            password (str): Password of the MongoDB.
            database (str): Database name of the MongoDB.
            collection (str): Collection name of the MongoDB.
            port (int, optional): Port of the MongoDB. Defaults to 27017.
        """
        self._hostname = hostname
        self._user = user
        self._password = password
        self._database = database
        self._collection = collection
        self._port = int(port)

        self._client = MongoClient(
            host=self._hostname,
            port=self._port,
            username=self._user,
            password=self._password,
        )
        self._db = self._client[self._database]
        self._collection = self._db[self._collection]
        self._cursor = None

    def fetch_batch(
        self, batch_size: int, date_field: str = None, date_lower_bound: datetime = None
    ):
        query = {}
        if date_field and date_lower_bound:
            query[date_field] = {"$gt": date_lower_bound}

        if self._cursor is None:
            self._cursor = self._collection.find(query)

        documents = []
        for _ in range(batch_size):
            try:
                documents.append(self._clean_id(self._cursor.next()))
            except StopIteration:
                break
        return documents

    def fetch_all(self, date_field: str = None, date_lower_bound: datetime = None):
        query = {}
        if date_field and date_lower_bound:
            query[date_field] = {"$gt": date_lower_bound}

        if self._cursor is None:
            self._cursor = self._collection.find(query)
        else:
            self._cursor = self._cursor.clone()
        documents = [self._clean_id(doc) for doc in list(self._cursor)]
        return documents

    def get_fields(self):
        sample_doc = self._collection.find_one()
        if sample_doc:
            return list(sample_doc.keys())
        else:
            return []

    def _clean_id(self, document):
        document["_id"] = str(document["_id"])
        return document
