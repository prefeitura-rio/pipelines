# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.utils.dump_datario.utils import generate_dump_datario_schedules

#####################################
#
# datario Schedules
#
#####################################

dados_mestres_tables = {
    "bairro": {
        "url": "https://opendata.arcgis.com/datasets/dc94b29fc3594a5bb4d297bee0c9a3f2_3.geojson",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
    },
    "logradouro": {
        "url": "https://opendata.arcgis.com/datasets/899168c8feab4230a9f795ed07cdde7b_0.geojson",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
    },
    "zoneamento_setores": {
        # source https://www.data.rio/datasets/PCRJ::setores-1/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/2c56c0ca74d74bcd8b44378e20af707f_1/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "dev",
    },
    "zoneamento_subzonas_subsetores": {
        # source https://www.data.rio/datasets/PCRJ::subzonas-e-subsetores-1/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/815b62790a0942519020d4ad28e99fee_2/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "dev",
    },
    "zoneamento_zonas": {
        # source https://www.data.rio/datasets/PCRJ::zonas-1/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/d668fb697bd245229d3165aed0ee5193_3/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "dev",
    },
    "edificacoes": {
        "url": "https://opendata.arcgis.com/api/v3/datasets/124f1607b57d4292af5c0dfc2ff8572c_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": False,
        "materialize_to_datario": False,
        "dump_to_gcs": False,
        "materialization_mode": "dev",
        "interval": timedelta(days=500),
    },
}


dados_mestresclocks = generate_dump_datario_schedules(
    interval=timedelta(days=365),
    start_date=datetime(2022, 11, 9, 18, 10, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
    table_parameters=dados_mestres_tables,
    runs_interval_minutes=15,
)

dados_mestresmonthly_update_schedule = Schedule(clocks=untuple(dados_mestresclocks))
