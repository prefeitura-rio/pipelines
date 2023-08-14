# -*- coding: utf-8 -*-
from datetime import timedelta, datetime
from pytz import timezone
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.constants import constants


every_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_minute = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_10_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
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
            interval=timedelta(minutes=60),
            start_date=datetime(
                2021, 1, 1, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value,
            ],
        ),
    ]
)
