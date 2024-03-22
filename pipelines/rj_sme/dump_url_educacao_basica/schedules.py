# -*- coding: utf-8 -*-
# flake8: noqa: E501
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
# Disciplinas sem professor
#
#####################################

gsheets_urls = {
    "disciplinas_sem_professor": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1c5bSsVgmjb1m3z39cRlyKX9m3vhZHj5S/view?usp=share_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_alocacao",
    },
    "bimestral_2023": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1bC-I6mT9SdRVDDL583WpeK8WOJMuIhfz/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2022": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/19PFXJKvaOrbexnt_jA4otE-LnMfHUH0H/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
        "encoding": "latin-1",
        "on_bad_lines": "skip",
        "separator": ";",
    },
    "bimestral_2021": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1k-taU8bMEYJ2U5EHvrNWQZnzN2Ht3uso/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
        "encoding": "latin-1",
    },
    "bimestral_2019": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1Q_drlgajGOpSsNlqw1cV2pRJ30Oh47MJ/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2018": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1b7wyFsX6T4W6U_VWIjPmJZ4HI9btaLah/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2017": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1kclQeNuzDCy0Npny1ZZLPjqiPMScw_1P/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2016": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1QH9VsphqPvFwUfE7FgQYI6YJ4TJFTptv/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2015": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1VKDnvgOzrEdT5LkNYBDE_ayVvKsj5jR0/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2014": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/18pJonyKwV210dpXr_B2M0p708jYYGwKz/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2013": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1rSi-UgB3qZDLh8U3geKRkMgSdmxddO5v/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
    "bimestral_2012": {
        "dump_mode": "overwrite",
        "url": "https://drive.google.com/file/d/1scfnos9iER86QVMx7Y_qPM1SKVv0MUED/view?usp=drive_link",
        "url_type": "google_drive",
        "materialize_after_dump": True,
        "dataset_id": "educacao_basica_avaliacao",
    },
}

gsheets_clocks = generate_dump_url_schedules(
    interval=timedelta(days=365),
    start_date=datetime(2024, 3, 22, 12, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SME_AGENT_LABEL.value,
    ],
    dataset_id="",
    table_parameters=gsheets_urls,
)

gsheets_year_update_schedule = Schedule(clocks=untuple(gsheets_clocks))
