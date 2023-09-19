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

# TODO: Setar parametros e testar flow sem schedule ligado
gtfs_clocks = generate_execute_schedules(
    interval=timedelta(days=1),
    labels=[
        emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
    ],
    table_parameters=constants.GTFS_TABLES_PARAMS.value,
    dataset_id=constants.GTFS_DATASET_ID.value,
    runs_interval_minutes=15,
    start_date=None,
)

gtfs_captura_schedule = Schedule(clocks=untuple(gtfs_clocks))
