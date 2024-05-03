# -*- coding: utf-8 -*-
"""
Schedules for rj_smtr
"""

from datetime import timedelta, datetime
from pytz import timezone
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock, CronClock
from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.utils import generate_ftp_schedules
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

every_5_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_10_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=10),
            start_date=datetime(
                2021, 1, 1, 0, 0, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
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

ftp_clocks = generate_ftp_schedules(
    interval_minutes=60, label=emd_constants.RJ_SMTR_DEV_AGENT_LABEL
)
ftp_schedule = Schedule(ftp_clocks)
every_day_hour_five = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 5, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_day_hour_seven = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2022, 11, 30, 7, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)

every_dayofmonth_one_and_sixteen = Schedule(
    clocks=[
        CronClock(
            cron="0 12 16 * *",
            start_date=datetime(
                2022, 12, 16, 12, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
        CronClock(
            cron="0 12 1 * *",
            start_date=datetime(
                2023, 1, 1, 12, 0, tzinfo=timezone(constants.TIMEZONE.value)
            ),
            labels=[
                emd_constants.RJ_SMTR_AGENT_LABEL.value,
            ],
        ),
    ]
)
