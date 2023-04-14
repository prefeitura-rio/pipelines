# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_infra_edison.tasks import (
    coletaDado,
    #   trataDado,
    dataframe_to_csv,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: Formacao - Infraestrutura Edison Moreira",
    code_owners=["Edison Moreira"],
) as rj_escritorio_formacao_infra_edison_flow:

    df_transformado = coletaDado(10)
    dataframe_to_csv(df_transformado, "data.csv")

rj_escritorio_formacao_infra_edison_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_formacao_infra_edison_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
