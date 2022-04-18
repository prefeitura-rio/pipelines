# -*- coding: utf-8 -*-
"""
Flows for meteorologia_inmet
"""
import pendulum

from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.meteorologia_inmet.tasks import (slice_data, download,
                                                                    tratar_dados, salvar_dados)
from pipelines.rj_cor.meteorologia.meteorologia_inmet.schedules import hour_schedule
from pipelines.utils.tasks import (
    check_table_exists,
    create_bd_table,
    upload_to_gcs,
    dump_header_to_csv,
)
with Flow('COR: Meteorologia - Meteorologia INMET') as flow:
    # CURRENT_TIME = Parameter('CURRENT_TIME', default=None) or pendulum.now("utc")
    CURRENT_TIME = pendulum.now('UTC')  # segundo o manual é UTC

    DATASET_ID = 'meio_ambiente_clima'
    TABLE_ID = 'meteorologia_inmet'
    DUMP_TYPE = 'append'

    data, hora = slice_data(current_time=CURRENT_TIME)

    dados = download(data=data)
    dados = tratar_dados(dados=dados, hora=hora)
    path = salvar_dados(dados=dados)

    # Check if table exists
    EXISTS = check_table_exists(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        wait=path
    )

    # Create header and table if they don't exists
    with case(EXISTS, False):
        # Create CSV file with headers
        header_path = dump_header_to_csv(
            data_path=path,
            wait=EXISTS
        )

        # Create table in BigQuery
        create_db = create_bd_table(  # pylint: disable=invalid-name
            path=header_path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_type=DUMP_TYPE,
            wait=header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            wait=create_db,
        )

    with case(EXISTS, True):
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            wait=EXISTS
        )

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = hour_schedule
