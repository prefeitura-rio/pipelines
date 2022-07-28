# -*- coding: utf-8 -*-
"""
Constant values for the rj_cor.comando projects
"""

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the comando project
    """

    PATH_BASE_ENDERECOS = "/tmp/base_enderecos.csv"
    DATASET_ID = "administracao_servicos_publicos"
    TABLE_ID_EVENTOS = "eventos"
    TABLE_ID_ATIVIDADES_EVENTOS = "atividades_eventos"
    TABLE_ID_POPS = "procedimento_operacional_padrao"
