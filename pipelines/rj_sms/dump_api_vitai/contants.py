# -*- coding: utf-8 -*-
# pylint: disable=C0103
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """
    VAULT_PATH = "estoque_vitai"
    VAULT_KEY = "token"
    DATASET_ID = "dump_vitai"
    TABLE_POSICAO_ID = "estoque_posicao"
    TABLE_MOVIMENTOS_ID = "estoque_movimento"
