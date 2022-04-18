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
from basedosdados import Table

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
