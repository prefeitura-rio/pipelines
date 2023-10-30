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

    FTP_SERVER = "ftp2.datasus.gov.br"
    FTP_FILE_PATH = "/pub/sistemas/tup/downloads"
    BASE_FILE = "TabelaUnificada"
    DATASET_ID = "brutos_sigtap"
