"""
General utilities for interacting with dbt-rpc
"""

from dbt_client import DbtClient


def get_dbt_client(
    host: str = "dbt-rpc",
    port: int = 8580,
    jsonrpc_version: str = "2.0",
) -> DbtClient:
    return DbtClient(
        host=host,
        port=port,
        jsonrpc_version=jsonrpc_version,
    )
