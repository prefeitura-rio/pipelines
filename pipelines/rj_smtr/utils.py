# -*- coding: utf-8 -*-
"""
General purpose functions for rj_smtr
"""

###############################################################################
#
# Esse é um arquivo onde podem ser declaratas funções que serão usadas
# pelos projetos da rj_smtr.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar funções, basta fazer em código Python comum, como abaixo:
#
# ```
# def foo():
#     """
#     Function foo
#     """
#     print("foo")
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.utils import foo
# foo()
# ```
#
###############################################################################
from pathlib import Path

import basedosdados as bd
from basedosdados import Table
import pandas as pd


from pipelines.utils.utils import log


def create_or_append_table(dataset_id, table_id, path):
    """Conditionally create table or append data to its relative GCS folder.

    Args:
        dataset_id (str): target dataset_id on BigQuery
        table_id (str): target table_id on BigQuery
        path (str): Path to .csv data file
    """
    tb_obj = Table(table_id=table_id, dataset_id=dataset_id)
    if not tb_obj.table_exists("staging"):
        log("Table does not exist in STAGING, creating table...")
        tb_obj.create(
            path=path,
            if_table_exists="pass",
            if_storage_data_exists="replace",
            if_table_config_exists="replace",
        )
        log("Table created in STAGING")
    else:
        log("Table already exists in STAGING, appending to it...")
        tb_obj.append(filepath=path, if_exists="replace", timeout=600)
        log("Appended to table on STAGING successfully.")


# Removed due to future use of DBT for managing publishing
# if not tb_obj.table_exists("prod"):
#     log("Table does not exist in PROD, publishing...")
#     tb_obj.publish(if_exists="pass")
#     log("Published table in PROD successfully.")
# else:
#     log("Table already published in PROD.")


def generate_df_and_save(data: dict, fname: Path):
    """Save DataFrame as csv

    Args:
        data (dict): dict with the data which to build the DataFrame
        fname (Path): _description_
    """
    # Generate dataframe
    dataframe = pd.DataFrame()
    dataframe[data["key_column"]] = [
        piece[data["key_column"]] for piece in data["data"]
    ]
    dataframe["content"] = [piece for piece in data["data"]]

    # Save dataframe to CSV
    dataframe.to_csv(fname, index=False)


def bq_project(kind: str = "bigquery_prod"):
    """Get the set BigQuery project_id

    Args:
        kind (str, optional): Which client to get the project name from.
        Options are 'bigquery_staging', 'bigquery_prod' and 'storage_staging'
        Defaults to 'bigquery_prod'.

    Returns:
        str: the requested project_id
    """
    return bd.upload.base.Base().client[kind].project


def get_table_max_value(
    query_project_id: str, dataset_id: str, table_id: str, field_name: str, wait=None
):
    """Query a table to get the maximum value for the chosen field.
    Useful to incrementally materialize tables via DBT

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        field_name (str): column name to query
    """
    query = f"""
        SELECT
            max({field_name})
        FROM {query_project_id}.{dataset_id}.{table_id}
    """
    result = bd.read_sql(query=query, billing_project_id=bq_project())

    return result.iloc[0][0]
