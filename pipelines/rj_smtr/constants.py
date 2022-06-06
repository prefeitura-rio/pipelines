# -*- coding: utf-8 -*-
"""
Constant values for the rj_smtr projects
"""


###############################################################################
#
# Esse é um arquivo onde podem ser declaratas constantes que serão usadas
# por todos os projetos do rj_smtr.
#
# Por ser um arquivo opcional, pode ser removido sem prejuízo ao funcionamento
# do projeto, caos não esteja em uso.
#
# Para declarar constantes, basta fazer conforme o exemplo abaixo:
#
# ```
# class constants(Enum):
#     """
#     Constant values for the rj_smtr projects
#     """
#     FOO = "bar"
# ```
#
# Para usá-las, basta fazer conforme o exemplo abaixo:
#
# ```py
# from pipelines.rj_smtr.constants import constants
# print(constants.FOO.value)
# ```
#
###############################################################################

from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constant values for the rj_smtr projects
    """

    MAX_TIMEOUT_SECONDS = 60
    MAX_RETRIES = 3
    RETRY_DELAY = 10
    STPL_DATASET_ID = "br_rj_riodejaneiro_stpl_gps"
    STPL_TABLE_ID = "registros"
    REDIS_HOST = "localhost"
    TIMEZONE = "America/Sao_Paulo"
    CRITICAL_SECRET_PATH = "critical_webhook"
