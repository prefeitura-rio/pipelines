# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Inadimplente Schedules
#
#####################################

inadimplente_queries = {
    "chance": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_CHANCE;",
        "materialize_after_dump": True,
}

inadimplente_clocks = generate_dump_db_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2022, 10, 30, 23, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMFP_AGENT_LABEL.value,
    ],
    db_database="DBINAD",
    db_host="10.3.23.158",
    db_port="1433",
    db_type="sql_server",
    dataset_id="iptu_inadimplentes",
    vault_secret_path="formacao-iptu-inadimplentes",
    table_parameters=inadimplente_queries,
)

inadimplente_weekly_update_schedule = Schedule(clocks=untuple(inadimplente_clocks))
