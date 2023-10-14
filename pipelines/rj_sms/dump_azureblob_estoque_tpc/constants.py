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

    VAULT_PATH = "estoque_tpc"
    VAULT_KEY = "credential"
    DATASET_ID = "brutos_estoque_central_tpc"
    TABLE_POSICAO_ID = "estoque_posicao"
    CONTAINER_NAME = "datalaketpc"
    BLOB_PATH_POSICAO = "gold/logistico/cliente=prefeitura_rio/planta=sms_rio/estoque_local/estoque_local.csv"  # noqa: E501
