# -*- coding: utf-8 -*-
"""
Prefect flows for cor project
"""
from pipelines.rj_cor.bot_semaforo.flows import *
from pipelines.rj_cor.meteorologia.meteorologia_inmet.flows import *
from pipelines.rj_cor.meteorologia.meteorologia_redemet.flows import *
from pipelines.rj_cor.meteorologia.precipitacao_alertario.flows import *
from pipelines.rj_cor.meteorologia.precipitacao_cemaden.flows import *
from pipelines.rj_cor.meteorologia.precipitacao_inea.flows import *
from pipelines.rj_cor.meteorologia.satelite.flows import *
from pipelines.rj_cor.meteorologia.precipitacao_websirene.flows import *
from pipelines.rj_cor.meteorologia.radar.precipitacao.flows import *
from pipelines.rj_cor.comando.eventos.flows import *
