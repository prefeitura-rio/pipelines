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
# CETRIO Schedules
#
#####################################

cetrio_queries = {
    "pit": {
        "dump_mode": "overwrite",
        "execute_query": "SELECT f.CodCET AS cod_cet, \
            e.Logradouro, \
            e.BAIRRO, \
            e.NEQUIPEMP AS codigo_equipamento, \
            e.LOCEQUIP AS local_equipamento, \
            e.Latitude, \
            e.Longitude, \
            e.AP, d.Data, \
            t.Horario24 AS periodo, \
            CONVERT(time(0), t.PeriodoInicio) AS hora_inicio, \
            CONVERT(time(0), t.PeriodoTermino) AS hora_termino, \
            f.TotalRegistros AS total_registro, \
            f.TotalPlacas AS total_placas \
            FROM DWOCR.dbo.Fact_OCRS AS f \
            INNER JOIN DWOCR.dbo.Dim_Equipamento AS e \
                ON f.CodCET = e.CodCET \
            INNER JOIN DWOCR.dbo.Dim_Tempo AS t \
                ON t.CodTempo = f.CodTempo \
            INNER JOIN DWOCR.dbo.Dim_Data AS d \
                ON d.Data = t.Data;",
        "materialize_after_dump": True,
    },
}

cetrio_clocks = generate_dump_db_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 3, 1, 22, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value,
    ],
    db_database="DWOCR_Staging",
    db_host="10.39.64.50",
    db_port="1433",
    db_type="sql_server",
    dataset_id="cetrio",
    vault_secret_path="cet-rio-ocr-staging",
    table_parameters=cetrio_queries,
)

cetrio_daily_update_schedule = Schedule(clocks=untuple(cetrio_clocks))
