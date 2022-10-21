# -*- coding: utf-8 -*-
"""
Flows do Adalberto
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.tasks import create_table_and_upload_to_gcs

from pipelines.constants import constants
from pipelines.rj_smfp.aquecimento_adalberto.tasks import (
    download_data,
    parse_data,
    format_phone_field,
    save_report,
)
from pipelines.utils.decorators import Flow

with Flow("SMFP: GTIS3 - aquecimento_pipelines") as flow_aquecimento_adalberto:

    # Parâmetros
    n_users = Parameter("n_users", default=500)
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="prod")  # dev ou prod

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    format_phone_field(dataframe, "phone")
    format_phone_field(dataframe, "cell")
    tmp = save_report(dataframe)
    create_table_and_upload_to_gcs(
        data_path=tmp, dataset_id=dataset_id, table_id=table_id, dump_mode=dump_mode
    )

flow_aquecimento_adalberto.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_aquecimento_adalberto.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMFP_AGENT_LABEL.value],
)
