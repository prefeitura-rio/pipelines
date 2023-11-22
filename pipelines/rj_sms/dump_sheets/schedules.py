# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the sheets dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.rj_sms.dump_sheets.constants import constants as sheets_constants
from pipelines.utils.dump_url.utils import generate_dump_url_schedules
from pipelines.utils.utils import untuple_clocks as untuple


sms_parameters = {
    "estabelecimento_aux": {
        "url": "https://docs.google.com/spreadsheets/d/1EkYfxuN2bWD_q4OhHL8hJvbmQKmQKFrk0KLf6D7nKS4/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Sheet1",
        "dataset_id": sheets_constants.DATASET_ID.value,
        "table_id": "estabelecimento_auxiliar",
        "dump_mode": "overwrite",
    },
    "material_remume": {
        "url": "https://docs.google.com/spreadsheets/d/1p7tOI1VeeEgeuzP_mag5wGZHTetpb23g_ykwbcd2u00/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "CONSOLIDADO",
        "dataset_id": sheets_constants.DATASET_ID.value,
        "table_id": "material_remume",
        "dump_mode": "overwrite",
    },
}


sms_clocks = generate_dump_url_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 17, 45, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
    dataset_id=sheets_constants.DATASET_ID.value,
    table_parameters=sms_parameters,
)

sms_sheets_daily_update_schedule = Schedule(clocks=untuple(sms_clocks))
