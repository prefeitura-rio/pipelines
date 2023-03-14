# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Tasks for generating a data catalog from BigQuery.
"""
from google.cloud import bigquery
from prefect import task

from pipelines.rj_escritorio.data_catalog.utils import get_bigquery_client


@task
def list_tables(project_id: str, client: bigquery.Client = None):
    """
    List all datasets and tables in a project.

    Args:
        client: BigQuery client.
        project_id: Project ID.

    Returns:
        List of dictionaries in the format:
        {
            "project_id": "project_id",
            "dataset_id": "dataset_id",
            "table_id": "table_id",
            "url": "https://console.cloud.google.com/bigquery?p={project_id}&d={dataset_id}&t={table_id}&page=table",
            "private": True/False,
        }
    """
    if client is None:
        client = get_bigquery_client()
    tables = []
    for dataset in client.list_datasets(project=project_id):
        for table in client.list_tables(dataset):
            dataset_id = dataset.dataset_id
            table_id = table.table_id
            table_info = {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "url": f"https://console.cloud.google.com/bigquery?p={project_id}&d={dataset_id}&t={table_id}&page=table",
                "private": False if project_id == "datario" else True,
            }
            tables.append(table_info)
    return tables
