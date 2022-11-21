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
    DATASET_ID = "adm_cor_comando"
    TABLE_ID_EVENTOS = "ocorrencias"
    TABLE_ID_ATIVIDADES_EVENTOS = "ocorrencias_orgaos_responsaveis"
    TABLE_ID_POPS = "procedimento_operacional_padrao"
    TABLE_ID_ATIVIDADES_POPS = "procedimento_operacional_padrao_orgaos_responsaveis"
