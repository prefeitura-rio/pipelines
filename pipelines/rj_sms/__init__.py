# -*- coding: utf-8 -*-
"""
Prefect flows for rj_sms project
"""

from pipelines.rj_sms.dump_db_sivep.flows import *
from pipelines.rj_sms.dump_ftp_cnes.flows import *
from pipelines.rj_sms.dump_azureblob_estoque_tpc.flows import *
from pipelines.rj_sms.dump_api_prontuario_vitai.flows import *
from pipelines.rj_sms.dump_api_prontuario_vitacare.flows import *
from pipelines.rj_sms.dump_sheets.flows import *
from pipelines.rj_sms.dump_api_regulacao_sisreg.flows import *
