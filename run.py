# -*- coding: utf-8 -*-
from pipelines.rj_sms.farmacia_estoque.flows import captura_tpc
from pipelines.utils.utils import run_local
import argparse

# Create the parser
parser = argparse.ArgumentParser()
# Add an argument
parser.add_argument("--flow", type=str, required=True)
# Parse the argument
args = parser.parse_args()

if args.flow == "captura_tpc":
    run_local(
        captura_tpc, parameters={"container_name": "tpc", "blob_name": "report.csv"}
    )
