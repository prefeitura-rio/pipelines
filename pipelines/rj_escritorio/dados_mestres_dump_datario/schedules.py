# -*- coding: utf-8 -*-
# flake8: noqa: E501
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
        # source: https://www.data.rio/datasets/PCRJ::limite-de-bairros/about
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Cartografia/Limites_administrativos/MapServer/4/query?outFields=*&where=1%3D1&f=geojson",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
    },
    "logradouro": {
        # source: https://www.data.rio/datasets/PCRJ::logradouros/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/899168c8feab4230a9f795ed07cdde7b_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
    },
    "zoneamento": {
        # source: https://www.data.rio/maps/PCRJ::zoneamento-urbano-vigente/about
        # https://www.arcgis.com/home/item.html?id=23de66d35c394fc89a5c5bca844373b4
        "url": "https://pgeo3.rio.rj.gov.br/arcgis/rest/services/Urbanismo/LBB_Zoneamento_urbano_vigente/MapServer/0/query?outFields=*&where=1%3D1&f=geojson",  # noqa
        # "url": "https://opendata.arcgis.com/api/v3/datasets/23de66d35c394fc89a5c5bca844373b4/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
    },
    "edificacoes": {
        # source: https://www.data.rio/datasets/PCRJ::edifica%C3%A7%C3%B5es-2013-1/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/124f1607b57d4292af5c0dfc2ff8572c_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "interval": timedelta(days=500),
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
        "geometry_3d_to_2d": True,
    },
    "lote": {
        # source: https://www.data.rio/datasets/PCRJ::lotes-2013-1/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/e7a93d5c81844e07ab82ccfc45b436f4_1/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "interval": timedelta(days=500),
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
        "geometry_3d_to_2d": True,
    },
    "aeis": {
        # source: https://www.data.rio/datasets/PCRJ::aeis-%C3%A1rea-de-especial-interesse-social-smpu/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/98fc248a56724688b06d6611bdb2524d_1/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "interval": timedelta(days=500),
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
        "geometry_3d_to_2d": True,
    },
    "aeis_bairro_maravilha": {
        # source: https://www.data.rio/datasets/PCRJ::aeis-%C3%A1rea-de-especial-interesse-social-bairro-maravilha/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/ac12b0d378d44f75a86042aad13b8741_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "interval": timedelta(days=500),
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
        "geometry_3d_to_2d": True,
    },
    "subprefeitura": {
        # source: https://www.data.rio/datasets/PCRJ::limites-coordenadorias-especiais-dos-bairros-subprefeituras/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/e178d4b87fc94d389c73992263024e79_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
    },
    "numero_porta": {
        # source: https://www.data.rio/datasets/PCRJ::n%C3%BAmero-de-porta-endere%C3%A7os/about
        "url": "https://opendata.arcgis.com/api/v3/datasets/dcd2a72a81f44727ace876e893636009_0/downloads/data?format=geojson&spatialRefId=4326&where=1%3D1",  # noqa
        "dataset_id": "dados_mestres",
        "dump_mode": "overwrite",
        "materialize_after_dump": True,
        "materialize_to_datario": True,
        "dump_to_gcs": True,
        "materialization_mode": "dev",
        "convert_to_crs_4326": True,
        "geometry_column": "geometry",
    },
}


dados_mestresclocks = generate_dump_datario_schedules(
    interval=timedelta(days=365),
    start_date=datetime(
        2022, 12, 20, 17, 35, tzinfo=pytz.timezone("America/Sao_Paulo")
    ),
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
    table_parameters=dados_mestres_tables,
    runs_interval_minutes=15,
)

dados_mestresmonthly_update_schedule = Schedule(clocks=untuple(dados_mestresclocks))
