# -*- coding: utf-8 -*-
"""
Helper functions for generating a data catalog from BigQuery.
"""
from google.cloud import bigquery

from pipelines.utils.utils import get_credentials_from_env


def get_bigquery_client(mode: str = "prod") -> bigquery.Client:
    """
    Get BigQuery client.

    Returns:
        BigQuery client.
    """
    credentials = get_credentials_from_env(mode=mode)
    client = bigquery.Client(credentials=credentials)
    return client
