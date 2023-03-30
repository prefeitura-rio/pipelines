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
    client = bd.Storage().client["storage_staging"]
    blobs = client.list_blobs(
        bucket_or_name=client.bucket, prefix=constants.GTFS_STORAGE_FOLDER.value
    )
    module_path = "data/" + '-'.join(__name__.split('.')[1:-1])
    for blob in blobs:
        file_path = f"{module_path}/downloaded/{blob.name}"
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
    tables = {}
    for table_name in constants.GTFS_TABLE_NAMES.value:
        tb1 = pd.read_csv(f"{dir_paths[0]}/{table_name}.txt")
        tb2 = pd.read_csv(f"{dir_paths[1]}/{table_name}.txt")
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
    tables["routes"] = tables["routes"][
        ~ tables["routes"].route_short_name.astype(str).str.contains("SN")].copy()

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
            tables[table_name] = main_table[mask]

    # enforce type of columns - it affects rename_trip_id
    for table_name, table in tables.items():
        map_col_type = constants.GTFS_COLUMN_TYPE.value.get(table_name)
        if map_col_type:
            tables[table_name] = table.astype(map_col_type)
            

    trips = tables["trips"]

    def rename_trip_id(trip_id:str) -> str:
        """Returns: trip_short_name + direction_id + service_id"""
        trip = trips[trips.trip_id == trip_id].iloc[0]
        return f"{trip.trip_short_name}{trip.direction_id}{trip.service_id}"
    trip_id_dict = dict(zip(trips['trip_id'], trips['trip_id'].apply(rename_trip_id)))


    # General treatment
    for table_name, table in tables.items():

        # rename trip_id
        if "trip_id" in table.columns:
            table.trip_id = table.trip_id.map(trip_id_dict)

        # drop primary key duplicates
        table.drop_duplicates(constants.GTFS_PK_SUBSET.value[table_name], keep='first', inplace=True)

        # rename columns
        if constants.GTFS_RENAME_COLUMNS.value.get(table_name):
            table.rename(
                columns=constants.GTFS_RENAME_COLUMNS.value[table_name],
                inplace=True
            )

        # Save local for later import
        module_path = "data/" + '-'.join(__name__.split('.')[1:-1])
        table_path = f"{module_path}/treated"
        Path(table_path).mkdir(parents=True, exist_ok=True)
        table_path += f"/{table_name}.csv"
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
    cursor = pgdb.get_cursor()

    for table_name in constants.GTFS_TABLE_NAMES.value:  # pylint: disable=W0612
        table_db_name = constants.GTFS_RENAME_TABLES.value[table_name]

    for table_name in constants.GTFS_TABLE_NAMES.value:  # pylint: disable=W0612
        table_db_name = constants.GTFS_RENAME_TABLES.value[table_name]
        # empty table
        cursor.execute(f"TRUNCATE {table_db_name} CASCADE")
        # insert data
        log(f"Inserting in {table_db_name}...")
        with open(table_paths[table_name], 'r', encoding="utf8") as table_data:
            table_cols = table_data.readline().strip().split(',')
            table_data.seek(0)

            next(table_data)  # skip csv header
            sql = f"""
            COPY {table_db_name} ({','.join(table_cols)})
            FROM STDIN WITH CSV DELIMITER AS ','
            """
            cursor.copy_expert(sql, table_data)
            pgdb.commit()
        
        # TODO: expor banco de teste para conexão externa
        # TODO: definir método de buscar o gtfs
        # TODO: adicionar dados de conexão do banco ao vault
        pass
