# -*- coding: utf-8 -*-
"""
Schedules for br_rj_riodejaneiro_bilhetagem
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants as emd_constants
from pipelines.utils.utils import untuple_clocks as untuple

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.utils import (
    generate_execute_bilhetagem_schedules,
)

bilhetagem_clocks = generate_execute_bilhetagem_schedules(
    interval=timedelta(days=1),
    start_date=datetime(
        2020, 1, 1, tzinfo=pytz.timezone(emd_constants.DEFAULT_TIMEZONE.value)
    ),
    labels=[
        emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
    ],
    table_parameters=constants.BILHETAGEM_TABLES_PARAMS.value,
    runs_interval_minutes=15,
)

bilhetagem_daily_schedule = Schedule(clocks=untuple(bilhetagem_clocks))
