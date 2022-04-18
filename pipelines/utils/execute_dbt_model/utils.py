"""
General utilities for interacting with dbt-rpc
"""

from dbt_client import DbtClient


def get_dbt_client(
    host: str = "dbt-rpc",
    port: int = 8580,
    jsonrpc_version: str = "2.0",
) -> DbtClient:
    """
    Returns a DBT RPC client.

    Args:
        host: The hostname of the DBT RPC server.
        port: The port of the DBT RPC server.
        jsonrpc_version: The JSON-RPC version to use.

    Returns:
        A DBT RPC client.
    """
    return DbtClient(
        host=host,
        port=port,
        jsonrpc_version=jsonrpc_version,
    )
