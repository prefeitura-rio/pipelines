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
    },
    "metas_acordo_resultados_planilha": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1BnOqRKZSzlwyAWaWmaape9XS1Zrh_b-C6ApZjlaRo3E\
            /edit#gid=827909827",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "METAS_AR",
        "materialize_after_dump": True,
    },
    "orgaos": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1nqnwgigE0_Ac6-jkWiNwAdB4EMJfz1WV5nn0HaOhfu0\
            /edit#gid=1236673479",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Lista Órgãos",
        "materialize_after_dump": True,
    },
    "relacao_metas": {
        "dump_mode": "overwrite",
        "url": "https://docs.google.com/spreadsheets/d/1s9-PY6ayAYbtOY8_jUCvaMZSPP5xGdgNkERxS5QQCsw\
            /edit#gid=118781852",
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Relação MetaPE-AR2022",
        "gsheets_sheet_range": "A2:I1000",
        "materialize_after_dump": True,
    },
}


gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2022, 9, 29, 14, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    dataset_id="planejamento_gestao_dashboard_metas",
    table_parameters=gsheets_urls,
)

gsheets_daily_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
