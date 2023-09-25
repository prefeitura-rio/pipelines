# -*- coding: utf-8 -*-
# pylint: disable=C0103
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
            labels=[constants.INEA_AGENT_LABEL.value],
            parameter_defaults={
                "bucket_name": "rj-escritorio-dev",
                "convert_params": "-k=ODIM2.1 -M=All",
                "mode": "prod",
                "output_format": "HDF5",
                "prefix": "raw/meio_ambiente_clima/inea_radar_hdf5",
                "product": "ppi",
                "radar": "gua",
                "vols_remote_directory": "/var/opt/edge/vols",
            },
        )
    ]
)
every_5_minutes_mac = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[constants.INEA_AGENT_LABEL.value],
            parameter_defaults={
                "bucket_name": "rj-escritorio-dev",
                "convert_params": "-k=ODIM2.1 -M=All",
                "mode": "prod",
                "output_format": "HDF5",
                "prefix": "raw/meio_ambiente_clima/inea_radar_hdf5",
                "product": "ppi",
                "radar": "mac",
                "vols_remote_directory": "/var/opt/edge/vols",
            },
        )
    ]
)
every_1_day = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[constants.INEA_AGENT_LABEL.value],
            parameter_defaults={
                "bucket_name": "rj-escritorio-dev",
                "convert_params": "-k=ODIM2.1 -M=All",
                "mode": "prod",
                "output_format": "HDF5",
                "prefix": "raw/meio_ambiente_clima/inea_radar_hdf5",
                "product": "ppi",
                "radar": "gua",
                "get_only_last_file": False,
                "vols_remote_directory": "/var/opt/edge/vols",
            },
        )
    ]
)
every_1_day_mac = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(2021, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
            labels=[constants.INEA_AGENT_LABEL.value],
            parameter_defaults={
                "bucket_name": "rj-escritorio-dev",
                "convert_params": "-k=ODIM2.1 -M=All",
                "mode": "prod",
                "output_format": "HDF5",
                "prefix": "raw/meio_ambiente_clima/inea_radar_hdf5",
                "product": "ppi",
                "radar": "mac",
                "get_only_last_file": False,
                "vols_remote_directory": "/var/opt/edge/vols",
            },
        )
    ]
)
