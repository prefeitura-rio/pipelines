"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple


#####################################
#
# datario Schedules
#
#####################################

diretorio_tables = {
    "bairro": "https://opendata.arcgis.com/datasets/dc94b29fc3594a5bb4d297bee0c9a3f2_3.geojson",
    "logradouro": "https://opendata.arcgis.com/datasets/899168c8feab4230a9f795ed07cdde7b_0.geojson",
}

diretorio_clocks = [
    IntervalClock(
        interval=timedelta(days=30),
        start_date=datetime(2022, 3, 5, 1, 0, tzinfo=pytz.timezone("America/Sao_Paulo"))
        + timedelta(minutes=3 * count),
        # TODO change to RJ_DATARIO_AGENT_LABEL when it's ready
        labels=[
            constants.RJ_DATARIO_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "url": url,
            "dataset_id": "diretorio",
            "dump_type": "overwrite",
            "table_id": table_id,
        },
    )
    for count, (table_id, url) in enumerate(diretorio_tables.items())
]

diretorio_monthly_update_schedule = Schedule(clocks=untuple(diretorio_clocks))
