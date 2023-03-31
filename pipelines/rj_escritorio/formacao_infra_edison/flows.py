# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_infra_edison.tasks import (
    hello_name,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formacao - Infraestrutura Edison Moreira",
    code_owners=["gabriel", "diego", "edisonmoreira"],
) as rj_escritorio_formacao_infra_example_flow:
    name = Parameter("name", default="Cosmos")
    hello_name(name=name)


rj_escritorio_formacao_infra_example_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio_formacao_infra_example_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
