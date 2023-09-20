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
    clock_interval=BILHETAGEM_PRINCIPAL_INTERVAL,
    labels=[
        emd_constants.RJ_SMTR_AGENT_LABEL.value,
    ],
    runs_interval_minutes=15,
    table_parameters=constants.BILHETAGEM_TABLES_PARAMS.value,
    interval=BILHETAGEM_PRINCIPAL_INTERVAL.total_seconds(),
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
)

bilhetagem_principal_schedule = Schedule(clocks=untuple(bilhetagem_principal_clocks))

BILHETAGEM_TRANSACAO_INTERVAL = timedelta(minutes=1)
bilhetagem_transacao_clocks = generate_execute_schedules(
    clock_interval=BILHETAGEM_TRANSACAO_INTERVAL,
    labels=[
        emd_constants.RJ_SMTR_AGENT_LABEL.value,
    ],
    runs_interval_minutes=0,
    table_parameters=constants.BILHETAGEM_TRANSACAO_TABLE_PARAMS.value,
    interval=BILHETAGEM_TRANSACAO_INTERVAL.total_seconds(),
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
)

bilhetagem_transacao_schedule = Schedule(clocks=untuple(bilhetagem_transacao_clocks))

