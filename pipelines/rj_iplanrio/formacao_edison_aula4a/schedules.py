# -*- coding: utf-8 -*-
"""
Schedule Exemplo Carga DB EGPWEB Tables chances e comentarios para o Datalake
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from pipelines.constants import constants
from pipelines.utils.dump_db.utils import generate_dump_db_schedules
from pipelines.utils.utils import untuple_clocks as untuple

#####################################
#
# Shedule EGPWEB chances e comentarios
#
#####################################

_EGPWEB_queries = {
    "chance": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_CHANCE;",
    },
    "comentario": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT * FROM EGPWEB_PRD.dbo.VW_Comentario;",
    },
}

_EGPWEB_clocks = generate_dump_db_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2023, 3, 4, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_IPLANRIO_AGENT_LABEL.value,
    ],
    db_database="EGPWEB_PRD",
    db_host="10.2.221.101",
    db_port="1433",
    db_type="sql_server",
    dataset_id="formacao_egpweb_edison",
    vault_secret_path="egpweb-prod",
    table_parameters=_EGPWEB_queries,
)

_EGPWEB_weekly_update_schedule = Schedule(clocks=untuple(_EGPWEB_clocks))
