# -*- coding: utf-8 -*-
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
# EGPWeb Schedules
#
#####################################

gsheets_urls = {
    "metas_planejamento_estrategico": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1lWbNoBSPDLi7nhZvt1G3vEYBWF460Su8PKALXvQJH5w\
            /edit#gid=917050709",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "METAS CONSOLIDADO",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_dashboard_metas",
    },
    "metas_acordo_resultados_planilha": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1BnOqRKZSzlwyAWaWmaape9XS1Zrh_b-C6ApZjlaRo3E\
            /edit#gid=827909827",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "METAS_AR",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_dashboard_metas",
    },
    "orgaos": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1nqnwgigE0_Ac6-jkWiNwAdB4EMJfz1WV5nn0HaOhfu0\
            /edit#gid=1236673479",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Lista Órgãos",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_dashboard_metas",
    },
    "relacao_metas": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1s9-PY6ayAYbtOY8_jUCvaMZSPP5xGdgNkERxS5QQCsw\
            /edit#gid=118781852",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Relação MetaPE-AR2022",
        "gsheets_sheet_range": "A2:I1000",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_dashboard_metas",
    },
    "formato_acordo": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1Ax2kG7JtmBLcEKkdSFBKLvtvyYzARKzl-1wS6pCB0gs\
            /edit#gid=51595081",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "BASE_FORMATO_ACORDOS",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "acordo_resultado": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1PmVw9a_6-6D3MHskvLiV4z6PVJQYKaWYUPXFyHf88i8\
            /edit#gid=51595081",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "BASE_ACORDOS",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "meta_desdobrada": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1cEXVsFA1ngSDypr7-bSvxgJbklCXP-fMJomGQTexoic\
            /edit#gid=721056876",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "METAS_AR",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "recurso": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1chlhi4sCS8oeRnaS_rWEiaH53nT08lBpt3DI4UvhOuM\
            /edit#gid=1835916672",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Recursos",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "auditoria": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1WoaZLxqnSokfE4_XpYpy1T-GYCx7qNPiaQv8U485hOw\
            /edit#gid=1835916672",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Auditorias",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "avaliacao_meta": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/15WvSoNgLJlOCmlT881XqFw_tenjBPHyw0cGzR8eHzi4\
            /edit#gid=322217634",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Cores",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
    "metas_acordo_resultados_ordenacao": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1s9-PY6ayAYbtOY8_jUCvaMZSPP5xGdgNkERxS5QQCsw\
            /edit#gid=118781852",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Metas AR 2022",
        "gsheets_sheet_range": "A2:F1000",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_dashboard_metas",
    },
    "estimativa_premiacao": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1ikQC5tQKXCTmOfoi6B5Nxbb7OKs1Ubs9zNTYT0bwbCI\
            /edit#gid=322217634",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Estimativa Premiação",
        "materialize_after_dump": True,
        "dataset_id": "planejamento_gestao_acordo_resultados",
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2021, 11, 23, 12, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    table_parameters=gsheets_urls,
    dataset_id="",
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
