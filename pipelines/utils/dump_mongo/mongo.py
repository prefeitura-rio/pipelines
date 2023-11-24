# -*- coding: utf-8 -*-
from datetime import datetime

from pymongo import MongoClient

from pipelines.utils.utils import log


class Mongo:
    """
    MongoDB database.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        connection_string: str,
        database: str,
        collection: str,
    ) -> None:
        """
        Initializes the MongoDB database.

        Args:
            connection_string (str): MongoDB connection string.
            database (str): Database name.
            collection (str): Collection name.
        """
        self._client = MongoClient(connection_string)
        self._db = self._client[database]
        self._collection = self._db[collection]
        self._cursor = None

    def fetch_batch(
        self, batch_size: int, date_field: str = None, date_lower_bound: datetime = None
    ):
        query = {}
        if date_field and date_lower_bound:
            query[date_field] = {"$gt": date_lower_bound}

        log(
            f"Fetching batch of {batch_size} documents from MongoDB with query: {query}"
        )

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

        log(f"Fetching all documents from MongoDB with query: {query}")

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
