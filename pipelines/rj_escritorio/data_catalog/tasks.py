# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Tasks for generating a data catalog from BigQuery.
"""
from google.cloud import bigquery
import gspread
import pandas as pd
from prefect import task

from pipelines.rj_escritorio.data_catalog.utils import (
    get_bigquery_client,
    write_data_to_gsheets,
)
from pipelines.utils.utils import get_credentials_from_env, log


@task
def list_tables(project_id: str, client: bigquery.Client = None, mode: str = "prod"):
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
        log(f"Creating BigQuery client in mode {mode}.")
        client = get_bigquery_client(mode=mode)
    log(f"Listing tables in project {project_id}.")
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
def generate_dataframe_from_list_of_tables(list_of_tables: list) -> pd.DataFrame:
    """
    Generate a Pandas DataFrame from a list of tables.

    Args:
        list_of_tables: List of tables.

    Returns:
        Pandas DataFrame.
    """
    dataframe = pd.DataFrame(list_of_tables)
    log(f"Generated DataFrame with shape {dataframe.shape}.")
    return dataframe


@task
def update_gsheets_data_catalog(
    dataframe: pd.DataFrame, spreadsheet_url: str, sheet_name: str
) -> None:
    """
    Update a Google Sheets spreadsheet with a DataFrame.

    Args:
        dataframe: Pandas DataFrame.
        spreadsheet_url: Google Sheets spreadsheet URL.
        sheet_name: Google Sheets sheet name.
    """
    # Get gspread client
    credentials = get_credentials_from_env(
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    gspread_client = gspread.authorize(credentials)
    # Open spreadsheet
    log(f"Opening Google Sheets spreadsheet {spreadsheet_url} with sheet {sheet_name}.")
    sheet = gspread_client.open_by_url(spreadsheet_url)
    worksheet = sheet.worksheet(sheet_name)
    # Update spreadsheet
    log("Deleting old data.")
    worksheet.clear()
    log("Rewriting headers.")
    write_data_to_gsheets(
        worksheet=worksheet,
        data=[dataframe.columns.tolist()],
    )
    log("Updating new data.")
    write_data_to_gsheets(
        worksheet=worksheet,
        data=dataframe.values.tolist(),
        start_cell="A2",
    )
    # Add filters
    log("Adding filters.")
    first_col = "A"
    last_col = chr(ord(first_col) + len(dataframe.columns) - 1)
    worksheet.set_basic_filter(f"{first_col}:{last_col}")
    # Resize columns
    log("Resizing columns.")
    worksheet.columns_auto_resize(0, len(dataframe.columns) - 1)
    log("Done.")
