# -*- coding: utf-8 -*-
"""
Flow definition for generating a data catalog from BigQuery.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.data_catalog.schedules import update_data_catalog_schedule
from pipelines.rj_escritorio.data_catalog.tasks import (
    generate_dataframe_from_list_of_tables,
    list_tables,
    merge_list_of_list_of_tables,
    update_gsheets_data_catalog,
)
from pipelines.rj_escritorio.notify_flooding.tasks import (
    parse_comma_separated_string_to_list,
)
from pipelines.utils.decorators import Flow

with Flow(
    name="EMD: utils - Gerar catálogo de dados",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as rj_escritorio_data_catalog_flow:

    # Parameters
    project_ids = Parameter("project_ids")
    spreadsheet_url = Parameter("spreadsheet_url")
    sheet_name = Parameter("sheet_name")
    bq_client_mode = Parameter("bq_client_mode", default="prod")

    # Flow
    project_ids = parse_comma_separated_string_to_list(
        input_text=project_ids, output_type=str
    )
    list_of_list_of_tables = list_tables.map(
        project_id=project_ids, mode=unmapped(bq_client_mode)
    )
    list_of_tables = merge_list_of_list_of_tables(list_of_list_of_tables)
    dataframe = generate_dataframe_from_list_of_tables(list_of_tables)
    update_gsheets_data_catalog(
        dataframe=dataframe,
        spreadsheet_url=spreadsheet_url,
        sheet_name=sheet_name,
    )


rj_escritorio_data_catalog_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_data_catalog_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
rj_escritorio_data_catalog_flow.schedule = update_data_catalog_schedule
