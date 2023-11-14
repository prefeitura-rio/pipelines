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

    FTP_SERVER = "ftp.datasus.gov.br"
    FTP_FILE_PATH = "/cnes"
    BASE_FILE = "BASE_DE_DADOS_CNES"
    DATASET_ID = "brutos_cnes"
