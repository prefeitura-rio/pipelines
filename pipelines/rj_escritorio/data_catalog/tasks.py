# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Tasks for generating a data catalog from BigQuery.
"""
from typing import List

from arcgis.gis import Item
from google.cloud import bigquery
from prefect import task

from pipelines.rj_escritorio.data_catalog.constants import constants
from pipelines.rj_escritorio.data_catalog.utils import (
    build_items_data_from_metadata_json,
    create_or_update_item,
    fetch_api_metadata,
    get_bigquery_client,
    get_directory,
    get_all_items,
)
from pipelines.utils.utils import log


@task
def list_tables(  # pylint: disable=too-many-arguments
    project_id: str,
    client: bigquery.Client = None,
    mode: str = "prod",
    exclude_staging: bool = True,
    exclude_test: bool = True,
    exclude_logs: bool = True,
):
    """
    List all datasets and tables in a project.

    Args:
        client: BigQuery client.
        project_id: Project ID.
        mode: BigQuery client mode.
        exclude_staging: Exclude staging datasets.
        exclude_test: Exclude anything that contains the word "test".
        exclude_logs: Exclude log datasets.

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
        log(f"Creating BigQuery client in mode {mode}.")
        client = get_bigquery_client(mode=mode)
    log(f"Listing tables in project {project_id}.")
    tables = []
    for dataset in client.list_datasets(project=project_id):
        dataset_id: str = dataset.dataset_id
        if exclude_staging and dataset_id.endswith("_staging"):
            log(f"Excluding staging dataset {dataset_id}.")
            continue
        if exclude_test and "test" in dataset_id:
            log(f"Excluding test dataset {dataset_id}.")
            continue
        if exclude_logs and (
            dataset_id.startswith("logs_") or dataset_id.endswith("_logs")
        ):
            log(f"Excluding logs dataset {dataset_id}.")
            continue
        for table in client.list_tables(dataset):
            table_id = table.table_id
            if exclude_test and "test" in table_id:
                log(f"Excluding test table {table_id}.")
                continue
            table_info = {
                "project_id": project_id,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "url": f"https://console.cloud.google.com/bigquery?p={project_id}&d={dataset_id}&t={table_id}&page=table",
                "private": not project_id == "datario",
            }
            tables.append(table_info)
    log(f"Found {len(tables)} tables in project {project_id}.")
    return tables


@task
def merge_list_of_list_of_tables(list_of_list_of_tables: list) -> list:
    """
    Merge a list of list of tables into a single list of tables.

    Args:
        list_of_list_of_tables: List of list of tables.

    Returns:
        List of tables.
    """
    list_of_tables = [
        table for list_of_tables in list_of_list_of_tables for table in list_of_tables
    ]
    log(f"Merged {len(list_of_tables)} tables.")
    return list_of_tables


@task
def fetch_metadata(list_of_tables: list) -> list:
    """
    For each table in the list, fetches metadata from the metadata API.

    Args:
        list_of_tables: List of tables.

    Returns:
        List of tables with metadata. Each table is a dictionary in the format:
        {
            "project_id": "project_id",
            "dataset_id": "dataset_id",
            "table_id": "table_id",
            "url": "https://console.cloud.google.com/bigquery?p={project_id}&d={dataset_id}&t={table_id}&page=table",
            "private": True/False,
            "metadata": {
                "title": "Title",
                "short_description": "Short description",
                "long_description": "Long description",
                "update_frequency": "Update frequency",
                "temporal_coverage": "Temporal coverage",
                "data_owner": "Data owner",
                "publisher_name": "Publisher name",
                "publisher_email": "Publisher email",
                "tags": ["Tag1", "Tag2"],
                "categories": ["Category1", "Category2"],
                "columns": [
                    {
                        "name": "column_name",
                        "description": "Column description",
                    }
                ]
            }
        }
    """
    log(f"Fetching metadata for {len(list_of_tables)} tables.")
    remove_tables = []
    for table in list_of_tables:
        project_id = table["project_id"]
        dataset_id = table["dataset_id"]
        table_id = table["table_id"]
        try:
            table["metadata"] = fetch_api_metadata(
                project_id=project_id, dataset_id=dataset_id, table_id=table_id
            )
        except:  # pylint: disable=bare-except
            log(
                f"Error fetching metadata for {project_id}.{dataset_id}.{table_id}. Will exclude this table from the catalog."  # pylint: disable=line-too-long
            )
            remove_tables.append(table)
    for table in remove_tables:
        list_of_tables.remove(table)
    log(f"Fetched metadata for {len(list_of_tables)} tables.")
    return list_of_tables


@task
def update_datario_catalog(list_of_metadata: list):
    """
    Update the data.rio catalog with our tables.

    Args:
        list_of_metadata: List of tables with metadata.
    """
    log(f"Updating data.rio catalog with {len(list_of_metadata)} tables.")
    updated_items = []
    duplicates_list = constants.DONT_PUBLISH.value
    (
        items_data,
        categories,
        project_ids,
        dataset_ids,
        table_ids,
    ) = build_items_data_from_metadata_json(metadata=list_of_metadata)
    for item_data, item_categories, project_id, dataset_id, table_id in zip(
        items_data, categories, project_ids, dataset_ids, table_ids
    ):
        log(f"Updating table `{project_id}.{dataset_id}.{table_id}`")
        item: Item = create_or_update_item(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            data=item_data,
        )
        log(f"Created/updated item: ID={item.id}, Title={item.title}")
        item.share(everyone=True, org=True, groups=item_categories)
        log(f"Shared item: ID={item.id} with groups: {item_categories}")
        move_dir = get_directory(project_id, dataset_id, table_id, duplicates_list)
        item.move(move_dir)
        log(f"Moved item: ID={item.id} to {move_dir}/ directory")
        updated_items.append(item.id)
        all_items: List[Item] = get_all_items()
        not_updated_items = [item for item in all_items if item.id not in updated_items]
        log(f"Deleting {len(not_updated_items)} items.")
        for item in not_updated_items:
            item.delete()
            log(f"Deleted item: ID={item.id}")
