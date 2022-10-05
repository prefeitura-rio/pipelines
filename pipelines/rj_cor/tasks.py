# -*- coding: utf-8 -*-
# # pylint: disable=R0914,W0613,W0102
"""
Common  Tasks for rj-cor
"""

from prefect import task
from prefect.triggers import all_successful

from pipelines.rj_cor.utils import build_redis_key
from pipelines.utils.utils import get_redis_client


@task
def get_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    wait=None,
) -> list:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files_on_redis = redis_client.get(key)
    files_on_redis = [] if files_on_redis is None else files_on_redis
    files_on_redis = list(set(files_on_redis))
    files_on_redis.sort()
    return files_on_redis


@task(trigger=all_successful)
def save_on_redis(
    dataset_id: str, table_id: str, mode: str = "prod", files: list = [], wait=None
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client()
    key = build_redis_key(dataset_id, table_id, "files", mode)
    files = list(set(files))
    print(">>>> save on redis files ", files)
    files.sort()
    redis_client.set(key, files)
