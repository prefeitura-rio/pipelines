# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from copy import deepcopy

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants

# SMTR Imports #

from pipelines.rj_smtr.flows import captura_generico

from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.schedules import (
    bilhetagem_principal_schedule,
    bilhetagem_transacao_schedule,
)

# Flows #

# BILHETAGEM TRANSAÇÃO - CAPTURA A CADA MINUTO #

bilhetagem_transacao_captura = deepcopy(captura_generico)
bilhetagem_transacao_captura.name = "SMTR: Bilhetagem Transação (captura)"
bilhetagem_transacao_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
bilhetagem_transacao_captura.schedule = bilhetagem_transacao_schedule

# BILHETAGEM PRINCIPAL - CAPTURA DIÁRIA DE DIVERSAS TABELAS #

bilhetagem_principal_captura = deepcopy(captura_generico)
bilhetagem_principal_captura.name = "SMTR: Bilhetagem Principal (captura)"
bilhetagem_principal_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_principal_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
bilhetagem_principal_captura.schedule = bilhetagem_principal_schedule
