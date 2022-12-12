# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

# from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Eath Engine Asset creation schedules
#
#####################################


ee_daily_update_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),
            labels=[
                constants.RJ_SEOP_AGENT_LABEL.value,
            ],
        ),
    ]
)
