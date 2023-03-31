# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_infra.tasks import (
    hello_name,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formacao - Exemplo infra Ricardo Bandeira",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as rj_escritorio_formacao_ricardo_bandeira:
    name = Parameter("name", default="World")
    hello_name(name=name)


rj_escritorio_formacao_ricardo_bandeira.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_formacao_ricardo_bandeira.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
