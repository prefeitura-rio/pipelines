# -*- coding: utf-8 -*-
"""
Example flow
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.formacao import *
from pipelines.formacao.formacao_paty.tasks import (
    download_data,
    parse_data,
    save_report,
)
from pipelines.utils.decorators import Flow

with Flow("EMD: formacao - Exemplo de flow do Prefect") as formacao_exemplo_flow:

    # Parâmetros
    n_users = Parameter("n_users", default=10)

    # Tasks
    data = download_data(n_users)
    dataframe = parse_data(data)
    save_report(dataframe)

formacao_exemplo_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
formacao_exemplo_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
formacao_exemplo_flow.schedule = None