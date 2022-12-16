# -*- coding: utf-8 -*-
"""
Prefect flows for rj_smtr project
"""
###############################################################################
# Automatically managed, please do not touch
###############################################################################
from pipelines.rj_smtr.flows import *

from pipelines.rj_smtr.br_rj_riodejaneiro_rdo.flows import *
from pipelines.rj_smtr.br_rj_riodejaneiro_stpl_gps.flows import *
from pipelines.rj_smtr.br_rj_riodejaneiro_sigmob.flows import *
from pipelines.rj_smtr.br_rj_riodejaneiro_onibus_gps.flows import *
from pipelines.rj_smtr.br_rj_riodejaneiro_brt_gps.flows import *
from pipelines.rj_smtr.materialize_to_datario.flows import *
from pipelines.rj_smtr.registros_ocr_rir.flows import *
from pipelines.rj_smtr.projeto_subsidio_sppo.flows import *
