# -*- coding: utf-8 -*-
"""
Prefect flows for rj_smfp project
"""

from pipelines.rj_smfp.aquecimento_adalberto.flows import *
from pipelines.rj_smfp.dump_db_ergon.flows import *
from pipelines.rj_smfp.dump_db_ergon_comlurb.flows import *
from pipelines.rj_smfp.dump_db_metas.flows import *
from pipelines.rj_smfp.dump_inadimplente.flows import *
from pipelines.rj_smfp.dump_url_metas.flows import *
from pipelines.rj_smfp.goals_dashboard_dbt.flows import *
