# -*- coding: utf-8 -*-
"""
Schedules for rj_smtr
"""

from datetime import timedelta, datetime
from pytz import timezone
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.constants import constants

every_minute = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)
every_minute_dev = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_10_minutes_dev = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)


every_hour = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_hour_minute_six = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(
                2021, 1, 1, 0, 6, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)
