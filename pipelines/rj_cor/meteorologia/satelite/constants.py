# -*- coding: utf-8 -*-
# pylint: disable=c0103
"""
Constant values for the satelite tables
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the satelite project
    """

    DATASET_ID = "clima_satelite"
    VARIAVEL_RR = "RRQPEF"
    TABLE_ID_RR = "taxa_precipitacao_goes_16"
    VARIAVEL_TPW = "TPWF"
    TABLE_ID_TPW = "quantidade_agua_precipitavel_goes_16"
    VARIAVEL_cmip = "CMIPF"
    TABLE_ID_cmip = "infravermelho_longo_banda_13_goes_16"
