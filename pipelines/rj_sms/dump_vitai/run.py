# -*- coding: utf-8 -*-
from pipelines.rj_sms.dump_vitai.flows import dump_vitai
from pipelines.utils.utils import run_local

run_local(dump_vitai)
