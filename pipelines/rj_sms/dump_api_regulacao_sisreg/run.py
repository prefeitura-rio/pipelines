# -*- coding: utf-8 -*-
from flows import dump_sisreg_scheduled_patients
from pipelines.utils.utils import run_local

run_local(dump_sisreg_scheduled_patients)
