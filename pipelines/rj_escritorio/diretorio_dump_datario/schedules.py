# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.utils.dump_datario.utils import generate_dump_datario_schedules

#####################################
#
# datario Schedules
#
#####################################

diretorio_tables = {
    "bairro": {
        "url": "https://opendata.arcgis.com/datasets/dc94b29fc3594a5bb4d297bee0c9a3f2_3.geojson",
        "dataset_id": "diretorio",
        "dump_type": "overwrite",
    },
    "logradouro": {
        "url": "https://opendata.arcgis.com/datasets/899168c8feab4230a9f795ed07cdde7b_0.geojson",
        "dataset_id": "diretorio",
        "dump_type": "overwrite",
    },
}


diretorio_clocks = generate_dump_datario_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 3, 21, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
    table_parameters=diretorio_tables,
    runs_interval_minutes=15,
)

diretorio_monthly_update_schedule = Schedule(clocks=untuple(diretorio_clocks))
