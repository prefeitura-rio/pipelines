# -*- coding: utf-8 -*-
# pylint: disable=line-too-long
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Setur Schedules
#
#####################################

gsheets_urls = {
    "museu_do_amanha": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu do Amanhã",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "cristo_redentor": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Cristo Redentor (Paineiras)",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "bio_parque": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Bio Parque",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "aquario": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "AquaRio",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "ccbb": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "CCBB",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "iss": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "ISS",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "galeao": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Galeão",
        "gsheets_sheet_range": "A5:D1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    # "caged": {
    #     "dump_mode": "overwrite",
    #     "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
    #         /edit#gid=1289262407",
    #     "url_type": "google_sheet",
    #     "gsheets_sheet_name": "CAGED",
    #     "gsheets_sheet_range": "A5:F1000",
    #     "materialize_after_dump": True,
    #     "dataset_id": "turismo_fluxo_visitantes",
    # },
    "rede_hoteleira_ocupacao_geral": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Ocupação da Rede Hoteleira",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "rede_hoteleira_ocupacao_grandes_eventos": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Ocupação da Rede Hoteleira em Grandes Eventos",
        "gsheets_sheet_range": "A5:E1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "santos_dumont": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "SDU",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "novo_rio": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Novo Rio",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "pao_de_acucar": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Parque Bondinho - Pão de Açucar",
        "gsheets_sheet_range": "A4:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "museu_nacional": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu Histórico Nacional",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "museu_republica": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu da República",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "museu_chacara_do_ceu": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu Chácara do Céu",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "museu_acude": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu do Açude",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "museu_villa_lobos": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Museu Villa-Lobos",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
    "empregos_turismo": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1qajRmbb4rvMzDW-fk_BhUGPvYkVKmr17\
            /edit#gid=1289262407",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Empregos",
        "gsheets_sheet_range": "A5:B1000",
        "materialize_after_dump": True,
        "dataset_id": "turismo_fluxo_visitantes",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    runs_interval_minutes=1,
    start_date=datetime(2021, 12, 21, 6, 15, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SETUR_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="turismo_fluxo_visitantes",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
