"""
Schedules for emd
Rodar a cada hora no minuto 30 para garantir que 
temos o satélite já fez pelo menos um escaneamento
da superfície
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants

hour_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(hours=1),
            start_date=datetime(2021, 1, 1, 0, 30, 0),
            labels=[
                constants.K8S_AGENT_LABEL.value,
            ]
        ),
    ]
)
