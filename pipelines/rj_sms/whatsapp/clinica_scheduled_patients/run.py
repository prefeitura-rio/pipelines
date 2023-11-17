# -*- coding: utf-8 -*-
from flows import flow_clinica_scheduled_patients
from pipelines.utils.utils import run_local

run_local(flow_clinica_scheduled_patients)
