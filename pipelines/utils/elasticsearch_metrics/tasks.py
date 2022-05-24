# -*- coding: utf-8 -*-
"""
Tasks for handling Elasticsearch metrics
"""
from time import time
from typing import Any, Dict, List

from prefect import task

from pipelines.utils.elasticsearch_metrics.utils import (
    format_document,
    index_document,
)


@task(checkpoint=False)
def start_timer() -> float:
    """
    Starts a timer
    """
    return time()


@task(checkpoint=False)
def stop_timer(start_time: float) -> float:
    """
    Stops a timer
    """
    return time() - start_time


@task(checkpoint=False)
def format_metrics(
    **args,
):
    """
    Formats metrics to be indexed in Elasticsearch
    """
    return dict(**args)


@task(checkpoint=False)
def post_metrics(  # pylint: disable=too-many-arguments
    flow_name: str = None,
    labels: List[str] = None,
    event_type: str = None,
    dataset_id: str = None,
    table_id: str = None,
    metrics: Dict[str, Any] = None,
):
    """
    Posts metrics to Elasticsearch
    """
    document = format_document(
        flow_name=flow_name,
        labels=labels,
        event_type=event_type,
        dataset_id=dataset_id,
        table_id=table_id,
        metrics=metrics,
    )
    index_document(document)
