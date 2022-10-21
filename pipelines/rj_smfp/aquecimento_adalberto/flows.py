# -*- coding: utf-8 -*-
"""
Flows do Adalberto
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

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

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    format_phone_field(dataframe, "phone")
    format_phone_field(dataframe, "cell")
    save_report(dataframe)

flow_aquecimento_adalberto.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_aquecimento_adalberto.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMFP_AGENT_LABEL.value],
)
