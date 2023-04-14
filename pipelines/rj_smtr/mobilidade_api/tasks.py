# -*- coding: utf-8 -*-
"""
Tasks for mobilidade-api
"""

from typing import List, Dict
from zipfile import ZipFile
from pathlib import Path

import basedosdados as bd
from prefect import task
import pandas as pd

from pipelines.rj_smtr.constants import constants
from pipelines.utils.dump_db.db import Postgres
from pipelines.utils.utils import get_vault_secret, log


@task
def get_gtfs_zipfiles():
    """
    Download gtfs files from storage

    Args:
        storage_folder (str, optional): Defaults to "gtfs".
    """
    file_paths = []
    st = bd.Storage("", "")  # pylint: disable=C0103
    client = st.client["storage_staging"]
    blobs = client.list_blobs(
        bucket_or_name=st.bucket, prefix=constants.GTFS_STORAGE_FOLDER.value
    )
    dirpath = Path("./raw")
    dirpath.mkdir(parents=True, exist_ok=True)
    for blob in blobs:
        # get blob name as only filename
        if blob.name.endswith(".zip"):
            file_path = f"{dirpath.as_posix()}/{blob.name.split('/')[-1]}"
            blob.download_to_filename(file_path)
            file_paths.append(file_path)
    return file_paths


@task
def extract_gtfs(zip_path: str):
    """
    Extract zipfiles to local filesystem

    Args:
        zip_path (str): Path to zip file

    Returns:
        str: Path to folder of extracted zip
    """
    compressed_file = ZipFile(file=zip_path)
    contents = compressed_file.namelist()
    log(f"Will extract files from gtfs zipfile:\n{contents}")
    if contents[0].endswith("/"):
        compressed_file.extractall()
        compressed_file.close()
        # remove final '/' for easier path handling
        return contents[0].replace("/", "")
    # if there's no subfolder inside gtfs zip, create a subfolder
    # to extract files to
    extracted_dir_path = Path(zip_path).stem
    Path(extracted_dir_path).mkdir(parents=True, exist_ok=True)
    compressed_file.extractall(extracted_dir_path)
    compressed_file.close()
    return extracted_dir_path


@task
def concat_gtfs(dir_paths: List[str]) -> List[pd.DataFrame]:
    """
    Concat two dataframes from local files

    Args:
        dirpaths (List[str]): A list of csv file paths

    Returns:
        List[pd.DataFrame]: A list of concatenated table with duplicates
    """
    if len(dir_paths) > 2:
        log("There were more than 2 gtfs files")
        raise Exception
    # diferentiate between brt and sppo gtfs files
    path_dict = {}
    for path in dir_paths:
        if "brt" in path:
            path_dict["brt"] = path
        else:
            path_dict["sppo"] = path
    tables = {}
    for table_name in constants.GTFS_TABLE_NAMES.value:
        # merge tables in especifc order for better handling
        # when dropping duplicates
        tb1 = pd.read_csv(f"{path_dict['brt']}/{table_name}.txt")
        tb2 = pd.read_csv(f"{path_dict['sppo']}/{table_name}.txt")
        tb_concat = pd.concat([tb1, tb2], axis=0)
        tables[table_name] = tb_concat
    return tables


@task
def treat_gtfs_tables(tables: Dict[str, pd.DataFrame]) -> dict:
    """
    Treat tables to deal with Django restrictions. Saves tables to
    `treated/<table_name>` and returns the save paths for all tables
    that were treated

    Args:
        tables (Dict[str, pd.DataFrame]): A dict containing {<table name>: <table DataFrame>}

    Returns:
        dict: A dict containing {<table name>: <absolute path>}
    """
    table_paths = {}
    # Remove SN routes from table `routes`
    log("Removing SN routes")
    tables["routes"] = tables["routes"][
        ~tables["routes"].route_short_name.astype(str).str.contains("SN")
    ].copy()

    # Cross checks to remove invalid foreign keys
    log("Cross checking for foreign keys")
    for (
        table_name,
        fk_info,
    ) in constants.GTFS_FK_SUBSET.value.items():  # dict containing cross check refs
        main_table = tables[table_name]
        for fk_field, fk_table_info in fk_info.items():
            log(f'Table:{table_name}, Foreign table: {fk_table_info["table_name"]}')
            fk_table = tables[fk_table_info["table_name"]]
            # Check if the foreign key column on the main table has
            # a reference on the source table
            mask = main_table[fk_field].isin(fk_table[fk_table_info["field"]])
            log(f"Shape before: {main_table.shape}")
            tables[table_name] = main_table[mask]
            log(f"Shape after: {main_table.shape}")

    # General treatment
    for table_name, table in tables.items():
        log(f"Treating table {table_name}")

        # enforce type of columns
        map_col_type = constants.GTFS_COLUMN_TYPE.value.get(table_name)
        if map_col_type:
            for column, dtype in map_col_type.items():
                # enforce type for writing NaN to csv as blank field
                table[column] = pd.to_numeric(table[column], errors="coerce").astype(
                    dtype
                )

        # drop primary key duplicates
        log(f"Dropping duplicates for table {table_name}")
        log(f"Shape before: {table.shape}")
        # keeping first, being consistent with merge order @ concat_gtfs
        table.drop_duplicates(
            constants.GTFS_PK_SUBSET.value[table_name], keep="first", inplace=True
        )
        log(f"Shape after: {table.shape}")

        # rename columns
        if constants.GTFS_DJANGO_COLUMNS.value.get(table_name):
            table.rename(
                columns=constants.GTFS_DJANGO_COLUMNS.value[table_name], inplace=True
            )

        # Save local for later import
        Path("./treated").mkdir(parents=True, exist_ok=True)
        filepath = f"./treated/{table_name}.csv"
        table.to_csv(
            filepath,
            sep=",",
            index=False,
            na_rep="",
        )
        table_paths[table_name] = filepath

    return table_paths


@task
def execute_update(table_paths: Dict[str, str]):  # pylint: disable=W0613
    """
    Connect to Postgresql DB and update it's data basing itself on
    a insert-overwrite approach

    Args:
        table_paths (Dict[str, str]): _description_
    """
    db_data = get_vault_secret(constants.MOBILIDADE_DB.value)["data"]
    pgdb = Postgres(  # pylint: disable=W0612
        db_data["hostname"], db_data["username"], db_data["pw"], db_data["dbname"]
    )
    cursor = pgdb.get_cursor()

    # Tuncate all tables before updating, avoids duplicate key error
    for table_name in constants.GTFS_TABLE_NAMES.value:  # pylint: disable=W0612
        table_db_name = constants.GTFS_DJANGO_TABLES.value[table_name]
        # Remove old data
        log(f"Truncating table {table_db_name}")
        cursor.execute(f"TRUNCATE {table_db_name} RESTART IDENTITY CASCADE")
        pgdb.commit()
        # insert data
        log(f"Inserting in {table_db_name}...")
        with open(table_paths[table_name], "r", encoding="utf8") as table_data:
            table_cols = table_data.readline().strip().split(",")
            table_data.seek(0)

            next(table_data)  # skip csv header
            sql = f"""
            COPY {table_db_name} ({','.join(table_cols)})
            FROM STDIN WITH CSV DELIMITER AS ','
            """
            cursor.copy_expert(sql, table_data)
            pgdb.commit()
