# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_sigmob
"""
import traceback
from datetime import timedelta
from pathlib import Path

import jinja2
import pandas as pd
import pendulum
from prefect import task
import requests

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import generate_df_and_save, log_critical
from pipelines.utils.dump_db.db import Postgres
from pipelines.utils.utils import log, get_vault_secret


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def request_data(endpoints: dict):
    """Request data from multiple API's

    Args:
        endpoints (dict): dict contaning the API id, URL and unique_key column

    Raises:
        e: Any exception during the request. Usually timeouts

    Returns:
        dict: containing the paths for the data saved during each request
    """

    # Get resources
    run_date = pendulum.now(constants.TIMEZONE.value).date()

    # Initialize empty dict for storing file paths
    paths_dict = {}

    # Iterate over endpoints
    for key in endpoints.keys():
        log("#" * 80)
        log(f"KEY = {key}")

        # Start with empty contents, page count = 0 and file_id = 0
        contents = None
        file_id = 0
        page_count = 0

        # Setup a template for every CSV file
        path_template = jinja2.Template(
            "{{run_date}}/{{key}}/data_versao={{run_date}}/{{key}}_version-{{run_date}}-{{id}}.csv"
        )

        # The first next_page is the initial URL
        next_page = endpoints[key]["url"]

        # Iterate over pages
        while next_page:
            page_count += 1

            try:

                # Get data
                log(f"URL = {next_page}")
                data = requests.get(
                    next_page, timeout=constants.SIGMOB_GET_REQUESTS_TIMEOUT.value
                )

                # Raise exception if not 200
                data.raise_for_status()
                data = data.json()

                # Store contents
                if contents is None:
                    contents = {
                        "data": data["result"] if "result" in data else data["data"],
                        "key_column": endpoints[key]["key_column"],
                    }
                else:
                    contents["data"].extend(data["data"])  # pylint: disable=E1136

                # Get next page
                if "next" in data and data["next"] != "EOF" and data["next"] != "":
                    next_page = data["next"]
                else:
                    next_page = None

            except Exception as unknown_error:
                err = traceback.format_exc()
                log(err)
                log_critical(f"Failed to request data from SIGMOB: \n{err}")
                raise unknown_error

            # Create a new file for every (constants.SIGMOB_PAGES_FOR_CSV_FILE.value) pages
            if page_count % constants.SIGMOB_PAGES_FOR_CSV_FILE.value == 0:

                # Increment file ID
                file_id += 1
                # "{{run_date}}/{{key}}/data_versao={{run_date}}/{{key}}_version-{{run_date}}-{{file_id}}.csv"
                path = Path(
                    path_template.render(
                        run_date=run_date, key=key, id="{:03}".format(file_id)
                    )
                )
                log(f"Reached page count {page_count}, saving file at {path}")

                # If it's the first file, create directories and save path
                if file_id == 1:
                    paths_dict[key] = path
                    path.parent.mkdir(parents=True, exist_ok=True)

                # Save CSV file
                generate_df_and_save(contents, path)

                # Reset contents
                contents = None

        # Save last file
        if contents is not None:
            file_id += 1
            path = Path(
                path_template.render(
                    run_date=run_date, key=key, id="{:03}".format(file_id)
                )
            )
            if file_id == 1:
                paths_dict[key] = path
                path.parent.mkdir(parents=True, exist_ok=True)
            generate_df_and_save(contents, path)
            log(f"Saved last file with page count {page_count} at {path}")

        # Move on to the next endpoint.

    # Return paths
    return paths_dict


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def request_paginated_endpoint(url: str):
    """Request raw data from a single paginated endpoint.

    Args:
        url (str): paginated api to request data from

    Returns:
        List[Dict]: the .json formatted response
    """
    # For paginated API, first next page is the initial url
    next_page = url
    # contents will store all data successfully requested
    contents = None
    while next_page:
        log(f"Starting request for url: {next_page}")
        data = requests.get(
            next_page, timeout=constants.SIGMOB_GET_REQUESTS_TIMEOUT.value
        )
        # Raise any HTTP errors
        data.raise_for_status()
        data = data.json()
        if contents is None:
            contents = data["data"]
        else:
            contents.extend(data["data"])
        # Next page has to have a url
        if "next" in data and data["next"] != "EOF" and data["next"] != "":
            next_page = data["next"]
        # Else, break the loop
        else:
            log("No next page found...")
            next_page = None
        log(f"Request succeeded. Found {len(contents)} records")
    return contents


@task
def insert_into_db(secret_path: str, data: list) -> None:
    """Inserts data to a postgresql database

    Args:
        secret_path(str): path to secret containing the
        db connection info
        data(list): json formatted data to insert

    Returns:
        None
    """

    db_data = get_vault_secret(secret_path)["data"]
    db_obj = Postgres(
        hostname=db_data["hostname"],
        port=int(db_data["port"]),
        user=db_data["user"],
        password=db_data["password"],
        database=db_data["database"],
    )
    select_query = f"SELECT * FROM {db_data['table_id']}"
    # with db_obj.connect as conn:
    #     with conn.cursor as cursor:
    # Points the cursor to the right table
    db_obj.execute_query(select_query)
    # get columns to update from data
    columns = db_obj.get_columns()
    insert_query = f"""
    INSERT INTO {db_data['table_id']}
    VALUES {tuple(f"%({column})s" for column in columns)}
    """.replace(  # remove single quotes from str repr of the columns
        "'", ""
    )
    insert_data = [
        {col: piece[col] if col in piece.keys() else "" for col in columns}
        for piece in data
    ]
    db_records = pd.DataFrame(columns=columns, data=db_obj.fetch_all()).to_dict(
        orient="records"
    )
    log(f"Database currently has {len(db_records)} records")
    insert_data = [piece for piece in insert_data if piece not in db_records]
    log(f"Found {len(insert_data)} records to insert")
    for piece in insert_data:
        log(f"Will execute query:\n{insert_query}")
        log(f"with values: {piece}")
        db_obj.execute_query(insert_query, piece)
        db_obj.commit()
    # Points the cursor back to the table for a final check
    db_obj.execute_query(select_query)
    log(f"Database has {len(db_obj.fetch_all())} records after insert")
