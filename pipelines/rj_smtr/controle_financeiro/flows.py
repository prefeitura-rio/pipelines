# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""


from copy import deepcopy
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants


# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
)
from pipelines.rj_smtr.constants import constants

from pipelines.utils.utils import set_default_parameters
from pipelines.rj_smtr.schedules import every_day


# Flows #

controle_cct_cb_captura = deepcopy(default_capture_flow)
controle_cct_cb_captura.name = "SMTR: Controle Financeiro CB - Captura"
controle_cct_cb_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cb_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

controle_cct_cb_captura = set_default_parameters(
    flow=controle_cct_cb_captura,
    default_parameters=constants.CONTROLE_FINANCEIRO_CAPTURE_DEFAULT_PARAMS.value
    | constants.CONTROLE_FINANCEIRO_CB_CAPTURE_PARAMS.value,
)
controle_cct_cb_captura.schedule = every_day

controle_cct_cett_captura = deepcopy(default_capture_flow)
controle_cct_cett_captura.name = "SMTR: Controle Financeiro CETT - Captura"
controle_cct_cett_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cett_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)

controle_cct_cett_captura = set_default_parameters(
    flow=controle_cct_cett_captura,
    default_parameters=constants.CONTROLE_FINANCEIRO_CAPTURE_DEFAULT_PARAMS.value
    | constants.CONTROLE_FINANCEIRO_CETT_CAPTURE_PARAMS.value,
)
controle_cct_cett_captura.schedule = every_day
