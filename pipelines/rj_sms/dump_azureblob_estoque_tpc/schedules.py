# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the vitacare dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.rj_sms.dump_azureblob_estoque_tpc.constants import (
    constants as tpc_constants,
)
from pipelines.utils.utils import untuple_clocks as untuple
from pipelines.rj_sms.utils import generate_dump_api_schedules


flow_parameters = [
    {
        "table_id": "estoque_posicao",
        "dataset_id": tpc_constants.DATASET_ID.value,
        "blob_file": "posicao",
    },
    # {
    #    "table_id": "estoque_pedidos_abastecimento",
    #    "dataset_id": tpc_constants.DATASET_ID.value,
    #    "blob_file": "pedidos",
    # },
    # {
    #    "table_id": "estoque_recebimento",
    #    "dataset_id": tpc_constants.DATASET_ID.value,
    #    "blob_file": "recebimento",
    # },
]


tpc_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 13, 40, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=2,
)

tpc_daily_update_schedule = Schedule(clocks=untuple(tpc_clocks))
