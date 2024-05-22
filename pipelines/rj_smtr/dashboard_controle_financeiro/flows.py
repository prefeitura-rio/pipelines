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

# from pipelines.rj_smtr.dashboard_controle_financeiro.tasks import (
#     pre_treatment_controle_cct,
# )
# from pipelines.utils.decorators import Flow


# SMTR Imports #

from pipelines.rj_smtr.flows import (
    default_capture_flow,
    # default_materialization_flow,
)
from pipelines.rj_smtr.constants import constants


# from pipelines.rj_smtr.tasks import (
#     create_date_hour_partition,
#     create_local_partition_path,
#     get_current_timestamp,
#     get_raw,
#     parse_timestamp_to_string,
#     save_raw_local,
#     save_treated_local,
#     upload_logs_to_bq,
#     bq_upload,
# )
from pipelines.utils.utils import set_default_parameters


# Flows #

controle_cct_cb_captura = deepcopy(default_capture_flow)
controle_cct_cb_captura.name = f"SMTR: {constants.CONTROLE_FINANCEIRO_CB_PARAMS.value['dataset_id']} {constants.CONTROLE_FINANCEIRO_CB_PARAMS.value['table_id']} - Captura"
controle_cct_cb_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cb_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cb_captura = set_default_parameters(
    flow=controle_cct_cb_captura,
    default_parameters=constants.CONTROLE_FINANCEIRO_CAPTURE_DEFAULT_PARAMS.value
    | constants.CONTROLE_FINANCEIRO_CB_CAPTURE_PARAMS.value,
)
# controle_cct_cb_captura.schedule = every_minute

controle_cct_cett_captura = deepcopy(default_capture_flow)
controle_cct_cett_captura.name = f"SMTR: {constants.CONTROLE_FINANCEIRO_CETT_PARAMS.value['dataset_id']} {constants.CONTROLE_FINANCEIRO_CETT_PARAMS.value['table_id']} - Captura"
controle_cct_cett_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cett_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)

controle_cct_cett_captura = set_default_parameters(
    flow=controle_cct_cett_captura,
    default_parameters=constants.CONTROLE_FINANCEIRO_CAPTURE_DEFAULT_PARAMS.value
    | constants.CONTROLE_FINANCEIRO_CETT_CAPTURE_PARAMS.value,
)
# controle_cct_cett_captura.schedule = every_minute
