# -*- coding: utf-8 -*-
from pipelines.rj_sms.check_ip.flows import check_ip
from pipelines.utils.utils import run_local

run_local(check_ip)
