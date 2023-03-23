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
        storage_folder (str, optional): _description_. Defaults to "gtfs".
    """
    filepaths = []
    client = bd.Storage().client["storage_staging"]
    blobs = client.list_blobs(
        bucket_or_name=client.bucket, prefix=constants.GTFS_STORAGE_FOLDER.value
    )
    for blob in blobs:
        filepath = blob.name
        blob.download_to_filename(filepath)
        filepaths.append(filepath)
    return filepaths


@task
def extract_gtfs(filepath: str):
    """
    Extract zipfiles to local filesystem

    Args:
        filepath (str): _description_

    Returns:
        _type_: _description_
    """
    compressed_file = ZipFile(file=filepath)
    contents = compressed_file.namelist()
    print(f"Will extract files from gtfs zipfile:\n{contents}")
    if contents[0].endswith("/"):
        compressed_file.extractall()
        compressed_file.close()
        # remove final '/' for easier path handling
        return contents[0].replace("/", "")
    # if there's no subfolder inside gtfs zip, create a subfolder
    # to extract files to
    dirpath = Path(filepath).stem
    Path(dirpath).mkdir(parents=True, exist_ok=True)
    compressed_file.extractall(dirpath)
    compressed_file.close()
    return dirpath


@task
def concat_gtfs(dirpaths) -> List[pd.DataFrame]:
    """
    Concat two dataframes from local files

    Args:
        gtfs_dir1 (str): _description_
        gtfs_dir2 (str): _description_

    Returns:
        List[pd.DataFrame]: _description_
    """
    if len(dirpaths) > 2:
        log("There were more than 2 gtfs files")
        raise Exception
    tables = {}
    for table_name in constants.GTFS_TABLE_NAMES.value:
        tb1 = pd.read_csv(f"{dirpaths[0]}/{table_name}.txt")
        tb2 = pd.read_csv(f"{dirpaths[1]}/{table_name}.txt")
        tb_concat = pd.concat([tb1, tb2], axis=0)
        tables[table_name] = tb_concat
    return tables


@task
def treat_gtfs_tables(tables: Dict[str, pd.DataFrame]):
    """
    Treat tables to deal with Django restrictions. Saves tables to
    `treated/<table_name>` and returns the save paths for all tables
    that were treated

    Args:
        tables (Dict[str, pd.DataFrame]): _description_

    Returns:
        _type_: _description_
    """
    table_paths = {}
    # Remove SN routes from table `routes`
    tables["routes"] = tables["routes"][
        not tables["routes"].route_short_name.str.contains("SN")
    ]

    # Cross checks to remove invalid foreign keys
    for (
        table_name,
        fk_info,
    ) in constants.GTFS_FK_SUBSET.value.items():  # dict containing cross check refs
        main_table = tables[table_name]
        for fk_field, fk_table_info in fk_info.items():

            fk_table = tables[fk_table_info["table_name"]]
            # Check if the foreign key column on the main table has
            # a reference on the source table
            mask = main_table[fk_field].isin(fk_table[fk_table_info["field"]])
            main_table = main_table[mask]

    # General treatment
    for table_name, table in tables.items():

        # drop primary key duplicates
        table.drop_duplicates(constants.GTFS_PK_SUBSET.value[table_name])

        # rename columns
        table.rename(
            mapper=constants.GTFS_RENAME_COLUMNS.value[table_name], inplace=True
        )
        # Save local for later import
        table_path = f"treated/{table_name}.csv"
        table.to_csv(table_path, sep=",", index=False)
        table_paths[table_name] = table_path

    return table_paths


@task
def execute_update(table_paths: Dict[str, str]):  # pylint: disable=W0613
    """
    Connect to Postgresql DB and update it's data basing itself on
    a insert-overwrite approach

    Args:
        table_paths (Dict[str, str]): _description_
    """
    db_data = get_vault_secret("TBD")  # TODO: add secret_path
    pgdb = Postgres(  # pylint: disable=W0612
        db_data["hostname"], db_data["username"], db_data["pw"], db_data["db_name"]
    )
    for table_name in constants.GTFS_TABLE_NAMES.value:  # pylint: disable=W0612
        cursor = pgdb.get_cursor()
        cursor.execute(f"TBD{table_name}")
        # insere a table
        # TRUNCATE TABLE f"pontos_{table_name}" CASCADE
        # COPY FROM {table_paths[table_name]}
        # TODO: expor banco de teste para conexão externa
        # TODO: adicionar as docstrings
        # TODO: definir método de buscar o gtfs
        # TODO: adicionar dados de conexão do banco ao vault
        pass
