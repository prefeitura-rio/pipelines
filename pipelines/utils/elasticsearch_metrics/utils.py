# -*- coding: utf-8 -*-
"""
Utilities for handling metrics with Elasticsearch
"""
import base64
import json
from typing import Any, Dict, List

from elasticsearch import Elasticsearch
import pendulum

from pipelines.utils.utils import get_vault_secret, log


def get_elasticsearch_client(
    es_config_secret_path: str = "elasticsearch-config",
) -> Elasticsearch:
    """
    Get an Elasticsearch client with configuration from Vault
    """
    try:
        es_config: str = get_vault_secret(es_config_secret_path)["data"]["config"]
    except Exception as exc:  # pylint: disable=broad-except
        log(f"Failed to get Elasticsearch config: {exc}", "error")
        return None
    try:
        es_config_dict: dict = json.loads(base64.b64decode(es_config.encode()).decode())
    except Exception as exc:  # pylint: disable=broad-except
        log(f"Failed to decode Elasticsearch config: {exc}", "error")
        return None
    return Elasticsearch(**es_config_dict)


def format_document(
    *,
    flow_name: str = None,
    labels: List[str] = None,
    event_type: str = None,
    dataset_id: str = None,
    table_id: str = None,
    metrics: Dict[str, Any] = None,
) -> dict:
    """
    Formats a document in a well-defined format for our Elasticsearch index
    """
    return {
        "timestamp": pendulum.now(tz="America/Sao_Paulo"),
        "flow_name": flow_name,
        "labels": labels,
        "event_type": event_type,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "metrics": metrics,
    }


def index_document(
    document: dict, es_client: Elasticsearch = None, index: str = "prefect-dados-rio"
) -> dict:
    """
    Indexes a document in Elasticsearch
    """
    if not es_client:
        es_client = get_elasticsearch_client()
    if not es_client:
        log("Impossible to index document, no Elasticsearch client available", "error")
        return None
    try:
        es_client.index(index=index, document=document)
    except Exception as exc:  # pylint: disable=broad-except
        log(f"Failed to index document: {exc}", "error")
        return None
    return document
