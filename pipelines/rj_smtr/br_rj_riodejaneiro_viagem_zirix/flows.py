# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_onibus_gps
"""
from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


# EMD Imports #

from pipelines.constants import constants as emd_constants

from pipelines.utils.decorators import Flow
from pipelines.utils.utils import set_default_parameters

# from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.flows import default_capture_flow


from pipelines.rj_smtr.br_rj_riodejaneiro_viagem_zirix.constants import (
    constants as zirix_constants,
)

from pipelines.rj_smtr.schedules import (
    every_10_minutes,
)

# from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

# Flows #

viagens_captura = deepcopy(default_capture_flow)
viagens_captura.name = "SMTR: Viagens Ônibus Zirix - Captura"
viagens_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
viagens_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

viagens_captura = set_default_parameters(
    flow=viagens_captura,
    default_parameters=zirix_constants.VIAGEM_CAPTURE_PARAMETERS.value,
)

viagens_captura.schedule = every_10_minutes
