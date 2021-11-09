"""
Provides multiple schedules for the EMD flows.
"""
from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from emd_flows.constants import constants

# Every minute
minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=1),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.K8S_AGENT_LABEL.value,
            ]
        ),
    ]
)

# Every 5 minutes
five_minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.K8S_AGENT_LABEL.value,
            ]
        ),
    ]
)

# Every 15 minutes
fifteen_minute_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=15),
            start_date=datetime(2021, 1, 1),
            labels=[
                constants.K8S_AGENT_LABEL.value,
            ]
        ),
    ]
)
