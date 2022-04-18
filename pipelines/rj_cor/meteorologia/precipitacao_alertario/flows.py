# -*- coding: utf-8 -*-
"""
Flows for precipitacao_alertario
"""
from prefect import Flow, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_cor.meteorologia.precipitacao_alertario.tasks import (
    download,
    tratar_dados,
    salvar_dados,
)
from pipelines.rj_cor.meteorologia.precipitacao_alertario.schedules import (
    minute_schedule,
)
from pipelines.utils.tasks import (
    check_table_exists,
    create_bd_table,
    upload_to_gcs,
    dump_header_to_csv,
)

with Flow("COR: Meteorologia - Pluviometria ALERTARIO") as flow:

    DATASET_ID = "meio_ambiente_clima"
    TABLE_ID = "precipitacao_alertario"
    DUMP_TYPE = "append"

    filename, current_time = download()
    dados = tratar_dados(filename=filename)
    path, partitions = salvar_dados(dados=dados, current_time=current_time)

    # Check if table exists
    EXISTS = check_table_exists(dataset_id=DATASET_ID, table_id=TABLE_ID)

    # Create header and table if they don't exists
    with case(EXISTS, False):
        # Create CSV file with headers
        header_path = dump_header_to_csv(data_path=path)

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
            partitions=partitions,
            wait=create_db,
        )

    with case(EXISTS, True):
        # Upload to GCS
        upload_to_gcs(
            path=path,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            partitions=partitions,
        )

# para rodar na cloud
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow.schedule = minute_schedule
