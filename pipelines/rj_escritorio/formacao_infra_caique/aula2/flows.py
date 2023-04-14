# -*- coding: utf-8 -*-

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.formacao_infra_caique.aula2.tasks import (
    GetApiRndUser,
    FormtaTelefoneDf,
    PlotaGrafico,
    GroupByCountryState,
)
from pipelines.utils.decorators import Flow


with Flow(
    "EMD: Flow do exercício da aula 2 do curso de formação em infraestrutura - Caique",
    code_owners=[
        "gabriel",
        "diego",
    ],
) as rj_escritorio_formacao_infra_caique_aula2_flow:

    # Parâmetros: nreg=50, inc='', gender='', nat=' '

    nreg = Parameter("nreg", default=50)
    inc = Parameter("inc", default="")
    gender = Parameter("gender", default="")
    nat = Parameter("nat", default="")

    data = GetApiRndUser(nreg=nreg, inc=inc, gender=gender, nat=nat)
    df = FormtaTelefoneDf(data)
    PlotaGrafico(df)
    GroupByCountryState(df)

rj_escritorio_formacao_infra_caique_aula2_flow.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
rj_escritorio_formacao_infra_caique_aula2_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
