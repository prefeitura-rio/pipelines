# -*- coding: utf-8 -*-
"""
Database dumping flows
"""

from functools import partial
from uuid import uuid4

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.tasks import (
    create_bd_table,
    upload_to_gcs,
    dump_header_to_csv,
)
from pipelines.utils.dump_datario.tasks import (
    get_datario_geodataframe,
)
from pipelines.utils.utils import notify_discord_on_failure

with Flow(
    name="EMD: template - Ingerir tabela do data.rio",
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
) as dump_datario_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # Datario
    url = Parameter("url")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    # overwrite or append
    dump_type = Parameter("dump_type", default="overwrite")

    #####################################
    #
    # Tasks section #1 - Create table
    #
    #####################################

    datario_path = get_datario_geodataframe(  # pylint: disable=invalid-name
        url=url, path=f"data/{uuid4()}/"
    )

    # Create CSV file with headers
    header_path = dump_header_to_csv(
        data_path=datario_path,
        wait=datario_path,
    )

    # Create table in BigQuery
    create_db = create_bd_table(  # pylint: disable=invalid-name
        path=header_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type=dump_type,
        wait=header_path,
    )

    #####################################
    #
    # Tasks section #2 - Dump batches
    #
    #####################################

    # Upload to GCS
    upload_to_gcs(
        path=datario_path,
        dataset_id=dataset_id,
        table_id=table_id,
        wait=create_db,
    )


dump_datario_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_datario_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
