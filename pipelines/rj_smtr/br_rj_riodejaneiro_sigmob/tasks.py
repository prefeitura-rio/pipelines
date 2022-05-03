# -*- coding: utf-8 -*-
"""
Tasks for br_rj_riodejaneiro_sigmob
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
import traceback
from datetime import timedelta
from pathlib import Path

#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################

import jinja2
import pendulum
import requests
from dbt_client import DbtClient
from prefect import task
from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.constants import constants
from pipelines.rj_smtr.utils import (
    bq_project,
    generate_df_and_save,
    get_table_max_value,
)
from pipelines.utils.utils import log


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
                    contents["data"].extend(data["data"])

                # Get next page
                if "next" in data and data["next"] != "EOF" and data["next"] != "":
                    next_page = data["next"]
                else:
                    next_page = None

            except Exception as e:
                err = traceback.format_exc()
                log(err)
                # log_critical(f"Failed to request data from SIGMOB: \n{err}")
                raise e

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


@task
def run_dbt_schema(
    client: DbtClient,
    dataset_id: str = "br_rj_riodejaneiro_sigmob",
    refresh: bool = False,
):
    """Run a whole schema (dataset) worth of models

    Args:
        client (DbtClient): Dbt interface of interaction
        dataset_id (str, optional): Dataset id on BigQuery. Defaults to "br_rj_riodejaneiro_sigmob".
        refresh (bool, optional): If true, rebuild all models from scratch. Defaults to False.

    Returns:
        None
    """
    run_command = f"run --select models/{dataset_id}"
    if refresh:
        run_command += "--full-refresh"
    client.cli(run_command)


@task
def build_incremental_model(
    dbt_client: DbtClient,
    dataset_id: str,
    base_table_id: str,
    mat_table_id: str,
    field_name: str = "data_versao",
    refresh: bool = False,
    wait=None,  # pylint: disable=unused-argument
):
    """_summary_

    Args:
        dbt_client (DbtClient): DBT interface object
        dataset_id (str): Dataset id on BigQuery
        base_table_id (str): Base table from which to materialize (usually, an external table)
        mat_table_id (str): Target table id for materialization
        field_name (str, optional): Key field (column) for dbt incremental filters.
        Defaults to "data_versao".
        refresh (bool, optional): If True, rebuild the table from scratch. Defaults to False.
        wait (NoneType, optional): Placeholder parameter, used to wait previous tasks finish.
        Defaults to None.

    Returns:
        bool: whether the table was fully built or not.
    """
    query_project_id = bq_project()
    last_mat_date = get_table_max_value(
        query_project_id, dataset_id, mat_table_id, field_name
    )
    last_base_date = get_table_max_value(
        query_project_id, dataset_id, base_table_id, field_name
    )

    run_command = f"run --select models/{dataset_id}/{mat_table_id}"
    if refresh:
        run_command += "--full-refresh"

    if last_base_date > last_mat_date:
        while last_base_date > last_mat_date:
            last_mat_date = get_table_max_value(
                query_project_id,
                dataset_id,
                mat_table_id,
                field_name,
                wait=dbt_client.cli(run_command),
            )
            if last_mat_date == last_base_date:
                return True
    return False
