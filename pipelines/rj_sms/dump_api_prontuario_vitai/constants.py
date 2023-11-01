# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for utils.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    VAULT_PATH = "estoque_vitai"
    VAULT_KEY = "token"
    DATASET_ID = "brutos_prontuario_vitai"
    TABLE_POSICAO_ID = "estoque_posicao"
    TABLE_MOVIMENTOS_ID = "estoque_movimento"