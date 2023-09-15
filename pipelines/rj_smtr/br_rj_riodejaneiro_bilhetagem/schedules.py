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

bilhetagem_principal_clocks = generate_execute_schedules(
    interval=timedelta(days=1),
    labels=[
        emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
    ],
    table_parameters=constants.BILHETAGEM_TABLES_PARAMS.value,
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
    runs_interval_minutes=15,
)

bilhetagem_principal_schedule = Schedule(clocks=untuple(bilhetagem_principal_clocks))

bilhetagem_transacao_clocks = generate_execute_schedules(
    interval=timedelta(minutes=1),
    labels=[
        emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
    ],
    table_parameters=constants.BILHETAGEM_TRANSACAO_TABLE_PARAMS.value,
    dataset_id=constants.BILHETAGEM_DATASET_ID.value,
    secret_path=constants.BILHETAGEM_SECRET_PATH.value,
    runs_interval_minutes=0,
)

bilhetagem_transacao_schedule = Schedule(clocks=untuple(bilhetagem_transacao_clocks))
