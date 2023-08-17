# -*- coding: utf-8 -*-
from pipelines.rj_sms.farmacia_estoque.flows import captura_tpc, captura_vitai, upload_organizations
from pipelines.utils.utils import run_local
import argparse

# Create the parser
parser = argparse.ArgumentParser()
# Add an argument
parser.add_argument("--flow", type=str, required=True)
# Parse the argument
args = parser.parse_args()

if args.flow == "captura_tpc":
    run_local(captura_tpc)
elif args.flow == "captura_vitai":
    run_local(captura_vitai)
elif args.flow == "upload_organizations":
    run_local(upload_organizations)