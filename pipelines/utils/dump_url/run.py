# -*- coding: utf-8 -*-
from pipelines.rj_sms.dump_datasus.flows import dump_datasus
from pipelines.utils.utils import run_local
import argparse



run_local(
    dump_datasus,# parameters={"container_name": "tpc", "blob_name": "report.csv"}
)
