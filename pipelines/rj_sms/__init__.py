# -*- coding: utf-8 -*-
"""
Prefect flows for rj_sms project
"""

from pipelines.rj_sms.dump_db_sivep.flows import *
from pipelines.rj_sms.dump_api_vitacare.flows import *
from pipelines.rj_sms.dump_prontuario_vitai.flows import *
from pipelines.rj_sms.materialize_tables.flows import *
