# -*- coding: utf-8 -*-
# flake8: noqa: E501
"""
Constants for the rain dashboard pipeline
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants for the rain dashboard pipeline
    """

    DATASET_ID = "clima_radar"
    TABLE_ID = "taxa_precipitacao_guaratiba"
