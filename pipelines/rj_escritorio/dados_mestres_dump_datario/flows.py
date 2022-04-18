# -*- coding: utf-8 -*-
"""
Database dumping flows
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.dump_datario.flows import dump_datario_flow
from pipelines.rj_escritorio.dados_mestres_dump_datario.schedules import (
    dados_mestresmonthly_update_schedule,
)

dump_dados_mestresflow = deepcopy(dump_datario_flow)
dump_dados_mestresflow.name = "EMD: dados_mestres - Ingerir tabelas do data.rio"
dump_dados_mestresflow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_dados_mestresflow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
dump_dados_mestresflow.schedule = dados_mestresmonthly_update_schedule
