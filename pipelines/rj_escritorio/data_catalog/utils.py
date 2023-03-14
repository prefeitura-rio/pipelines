# -*- coding: utf-8 -*-
"""
Helper functions for generating a data catalog from BigQuery.
"""
from typing import Any, List

from google.cloud import bigquery
from gspread.worksheet import Worksheet

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


def write_data_to_gsheets(
    worksheet: Worksheet, data: List[List[Any]], start_cell: str = "A1"
):
    """
    Write data to a Google Sheets worksheet.

    Args:
        worksheet: Google Sheets worksheet.
        data: List of lists of data.
        start_cell: Cell to start writing data.
    """
    try:
        start_letter = start_cell[0]
        start_row = int(start_cell[1:])
    except ValueError as exc:
        raise ValueError("Invalid start_cell. Please use a cell like A1.") from exc
    cols_len = len(data[0])
    rows_len = len(data)
    end_letter = chr(ord(start_letter) + cols_len - 1)
    if end_letter not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        raise ValueError("Too many columns. Please refactor this code.")
    end_row = start_row + rows_len - 1
    range_name = f"{start_letter}{start_row}:{end_letter}{end_row}"
    worksheet.update(range_name, data)
