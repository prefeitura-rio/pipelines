# -*- coding: utf-8 -*-
"""
Schedules for br_rj_riodejaneiro_bilhetagem
"""

from datetime import timedelta

from prefect.schedules import Schedule

from pipelines.constants import constants as emd_constants
from pipelines.utils.utils import untuple_clocks as untuple

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.utils import (
    generate_execute_schedules,
)

BILHETAGEM_PRINCIPAL_INTERVAL = timedelta(days=1)
bilhetagem_principal_clocks = generate_execute_schedules(
    clock_interval=timedelta(
        **constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["principal_run_interval"]
    ),
    labels=[
        emd_constants.RJ_SMTR_AGENT_LABEL.value,
    ],
    table_parameters=constants.BILHETAGEM_CAPTURE_PARAMS.value,
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
    source_type=constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["source_type"],
    runs_interval_minutes=constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value[
        "principal_runs_interval_minutes"
    ],
)

bilhetagem_principal_schedule = Schedule(clocks=untuple(bilhetagem_principal_clocks))

BILHETAGEM_TRANSACAO_INTERVAL = timedelta(minutes=1)
bilhetagem_transacao_clocks = generate_execute_schedules(
    clock_interval=timedelta(
        **constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["transacao_run_interval"]
    ),
    labels=[
        emd_constants.RJ_SMTR_AGENT_LABEL.value,
    ],
    table_parameters=constants.BILHETAGEM_TRANSACAO_CAPTURE_PARAMS.value,
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
    source_type=constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value["source_type"],
    runs_interval_minutes=constants.BILHETAGEM_GENERAL_CAPTURE_PARAMS.value[
        "transacao_runs_interval_minutes"
    ],
)

bilhetagem_transacao_schedule = Schedule(clocks=untuple(bilhetagem_transacao_clocks))

# bilhetagem_materializacao_clocks = generate_execute_schedules(
#     clock_interval=timedelta(hours=1),
#     labels=[
#         emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
#     ],
#     runs_interval_minutes=0,
#     table_parameters=constants.BILHETAGEM_MATERIALIZACAO_PARAMS.value,
#     dataset_id=constants.BILHETAGEM_DATASET_ID.value,
# )

# bilhetagem_materializacao_schedule = Schedule(
#     clocks=untuple(bilhetagem_materializacao_clocks)
# )
