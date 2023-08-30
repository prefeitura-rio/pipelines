# -*- coding: utf-8 -*-
from pipelines.rj_sms.dump_sheets.flows import medicamentos_flow
from pipelines.utils.utils import run_local

run_local(medicamentos_flow)