# -*- coding: utf-8 -*-
# pylint: disable=R0914,W0613,W0102,R0913
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
    redis_client = get_redis_client(host="127.0.0.1")
    key = build_redis_key(dataset_id, table_id, "files", mode)
    # print(f"\n\n get from key {key} \n\n")
    files_on_redis = redis_client.get(key)
    # print("\n\n files saved on redis: ", files_on_redis, "\n\n")
    files_on_redis = [] if files_on_redis is None else files_on_redis
    files_on_redis = list(set(files_on_redis))
    files_on_redis.sort()
    # print("\n\n files saved on redis: ", files_on_redis, "\n\n")
    return files_on_redis


@task(trigger=all_successful)
def save_on_redis(
    dataset_id: str,
    table_id: str,
    mode: str = "prod",
    files: list = [],
    keep_last: int = 50,
    wait=None,
) -> None:
    """
    Set the last updated time on Redis.
    """
    redis_client = get_redis_client(host="127.0.0.1")
    key = build_redis_key(dataset_id, table_id, "files", mode)
    # print(f"\n\n save on key {key} \n\n")
    files = list(set(files))
    files.sort(reverse=True)  # no backfill que roda contrário tem que ter o reverse
    # print("\n\n all files to save on redis: ", files, "\n\n")
    files = files[-keep_last:]
    # print("\n\n files to save on redis: ", files, "\n\n")
    redis_client.set(key, files)
