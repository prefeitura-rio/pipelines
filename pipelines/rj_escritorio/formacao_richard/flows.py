# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_richard.tasks import (
    obter_dados,
    converter_dados,
    converter_telefone,
    gerar_relatorio_grafico,
    agrupar,
    particionar_dados,
)
from pipelines.utils.decorators import Flow

with Flow(
    "EMD: formação - Richard - Aula 2",
    code_owners=[
        "gabriel",
    ],
    skip_if_running=True,
) as rj_escritorio_formacao_richard_users_flow:
    age_report = Parameter("age", default=0)
    group_by = Parameter("group_by", default=["location.country", "location.state"])
    number_users = Parameter("number_users", default=1000)
    partition_by = Parameter(
        "partition_by", default=["location.country", "location.state"]
    )
    convert_phone_numbers = Parameter("convert_phone_numbers", default=False)
    frequency_report = Parameter(
        "frequency_report", default=["gender", "location.country"]
    )

    dados = obter_dados(n=number_users)
    frame = converter_dados(dados=dados)
    if convert_phone_numbers:
        frame = converter_telefone(frame_in=frame)
    if frequency_report or age_report:
        gerar_relatorio_grafico(frame=frame, report=frequency_report, idades=age_report)
    if group_by:
        frame = agrupar(frame=frame, series=group_by)
    if partition_by:
        particionar_dados(frame=frame, series=partition_by)

rj_escritorio_formacao_richard_users_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio_formacao_richard_users_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
