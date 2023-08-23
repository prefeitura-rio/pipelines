# -*- coding: utf-8 -*-
from pipelines.rj_sms.dump_api_vitai.flows import dump_vitai
from pipelines.rj_sms.dump_azureblob_tpc.flows import dump_tpc
from pipelines.utils.utils import run_local

run_local(dump_vitai)
