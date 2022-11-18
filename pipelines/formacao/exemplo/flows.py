# -*- coding: utf-8 -*-
"""arquivo de tasks"""

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipeline.formacao.exemplo.tasks import download_data, to_dataframe, save_report
from pipelines.constants import constants

with Flow(
    "RIOAGUAS: Formacao Exemplo - Usuarios Aleatorios",
    code_owners=[
        "JP",
    ],
) as rioaguas_exemplo:
    # Parâmetros
    n_users = Parameter("n_users", default=100)

    # Tasks
    data = download_data(n_users)
    dataframe = to_dataframe(data)
    save_report(dataframe)

rioaguas_exemplo.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rioaguas_exemplo.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
rioaguas_exemplo.schedule = None
