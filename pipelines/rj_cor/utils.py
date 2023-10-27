# -*- coding: utf-8 -*-
"""
Utils for rj-cor
"""


def build_redis_key(dataset_id: str, table_id: str, name: str, mode: str = "prod"):
    """
    Helper function for building a key where to store the last updated time
    """
    key = dataset_id + "." + table_id + "." + name
    if mode == "dev":
        key = f"{mode}.{key}"
    return key
