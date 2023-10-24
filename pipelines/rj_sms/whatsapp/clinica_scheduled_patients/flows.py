# -*- coding: utf-8 -*-
"""
WhatsApp flow definition
"""
import pandas as pd
from prefect import Flow
from prefect.storage import GCS
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun

from tasks import (
    get_patients, 
    save_patients
)

from utils import (
    upload_to_datalake
)

with Flow("Vitacare patients") as flow_clinica_scheduled_patients:

    # Tasks
    get_patients()