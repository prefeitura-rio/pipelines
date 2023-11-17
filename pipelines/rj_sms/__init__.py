# -*- coding: utf-8 -*-
"""
Prefect flows for rj_sms project
"""

from pipelines.rj_sms.dump_db_sivep.flows import *
from pipelines.rj_sms.dump_ftp_cnes.flows import *
from pipelines.rj_sms.dump_api_prontuario_vitai.flows import *
from pipelines.rj_sms.whatsapp.clinica_scheduled_patients.flows import *
