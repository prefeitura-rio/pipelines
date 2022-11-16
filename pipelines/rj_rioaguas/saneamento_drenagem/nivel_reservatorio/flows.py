# -*- coding: utf-8 -*-

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.decorators import Flow
from pipelines.rj_rioaguas.saneamento_drenagem.nivel_reservatorio import (
    download_data,
    parse_data,
    save_report,
)


with Flow(
    "RIOAGUAS: nivel - Nivel dos reservatorios nas Prcs Varnhagen, Niteroi e Bandeira",
    code_owners=["Anderson"],
) as saneamento_drenagem_nivel_reservatorio:

    # Tasks
    data = download_data()
    dataframe = parse_data(data)
    save_report(dataframe)

saneamento_drenagem_nivel_reservatorio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
saneamento_drenagem_nivel_reservatorio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_RIOAGUAS_AGENT_LABEL.value]
)
saneamento_drenagem_nivel_reservatorio.schedule = None
