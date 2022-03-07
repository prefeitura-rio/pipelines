"""
Database dumping flows
"""

from copy import deepcopy

from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_datario.flows import dump_datario_flow
from pipelines.rj_escritorio.dump_datario.schedules import (
    diretorio_monthly_update_schedule,
)

dump_diretorio_flow = deepcopy(dump_datario_flow)
dump_diretorio_flow.name = "EMD: diretorio - Ingerir tabelas"
dump_diretorio_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_diretorio_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_diretorio_flow.schedule = diretorio_monthly_update_schedule
