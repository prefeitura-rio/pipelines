# -*- coding: utf-8 -*-
"""
Schedules for the INEA flows.
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants

every_5_minutes = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[
                constants.INEA_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "bucket_name": "rj-escritorio-dev",
                "convert_params": "-f=Whole -k=CFext -r=Short -p=Radar -M=All -z",
                "mode": "prod",
                "output_format": "NetCDF",
                "prefix": "raw/meio_ambiente_clima/inea_radar",
                "product": "ppi",
                "radar": "gua",
            },
        ),
    ]
)
