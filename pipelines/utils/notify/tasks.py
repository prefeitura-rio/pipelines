# -*- coding: utf-8 -*-
"""
Tasks for graphql
"""

###############################################################################
#
# Aqui é onde devem ser definidas as tasks para os flows do projeto.
# Cada task representa um passo da pipeline. Não é estritamente necessário
# tratar todas as exceções que podem ocorrer durante a execução de uma task,
# mas é recomendável, ainda que não vá implicar em  uma quebra no sistema.
# Mais informações sobre tasks podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/tasks.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# As tasks devem ser definidas como funções comuns ao Python, com o decorador
# @task acima. É recomendado inserir type hints para as variáveis.
#
# Um exemplo de task é o seguinte:
#
# -----------------------------------------------------------------------------
# from prefect import task
#
# @task
# def my_task(param1: str, param2: int) -> str:
#     """
#     My task description.
#     """
#     return f'{param1} {param2}'
# -----------------------------------------------------------------------------
#
# Você também pode usar pacotes Python arbitrários, como numpy, pandas, etc.
#
# -----------------------------------------------------------------------------
# from prefect import task
# import numpy as np
#
# @task
# def my_task(a: np.ndarray, b: np.ndarray) -> str:
#     """
#     My task description.
#     """
#     return np.add(a, b)
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################
from datetime import datetime, timedelta
from typing import Any, Dict
from pytz import timezone
from prefect import task, client
from pipelines.utils.utils import get_vault_secret, send_discord_message


@task
def query_flow_runs_by_state(
    _client: client.Client,
    prefix: str,
    state: str = "Failed",
    interval: dict = None,
    datetime_filter: str = None,
):
    """
    Query GraphQL API for failed flow runs. The query searches
    for flows with the given `prefix` and filter runs from the
    `datetime.now()` time up to the specified `interval` before.
    This utility can be used to query for any state by changing
    `state` arg.
    Args:
        client(prefect.client.Client): object used to interact with
        prefect backend.
        prefix(str): prefix to use in the query.
        state(Optional, str): flow run state to query for. Defaults
        to "Failed".
        interval(Optional, dict): dict of kwargs to pass to datetime.timedelta.
        Defaults to {"hours": 1},
        datetime_filter: str = None):
    Returns:
        dict: the response for the query

    """
    if datetime_filter:
        max_start_time = datetime.fromisoformat(datetime_filter)
    else:
        max_start_time = datetime.now(tz=timezone("America/Sao_Paulo"))
    if not interval:
        interval = dict(hours=1)
    min_start_time = max_start_time - timedelta(**interval)
    if not prefix.endswith("%"):
        prefix += "%"
    query = """
        query (
            $prefix:String!,
            $state:String!,
            $min_start_time:timestamptz!,
            $max_start_time:timestamptz)
        {
            flow(
                where:
                {
                    name: { _ilike:$prefix },
                    flow_runs: {
                    start_time: {_lte: $max_start_time, _gte:$min_start_time},
                    state: {_eq: $state }
                    }
                }
            ){
                name,
                project{
                name
                },
                flow_runs(
                    where:
                    {
                    start_time: {_lte: $max_start_time, _gte:$min_start_time},
                    state: {_eq: $state }
                    }
                    order_by:{name:asc, labels:asc}
                }
                ){
                    name,
                    scheduled_start_time,
                    state,
                    labels,
                    state_message
                }
            }
        }
    """
    response = _client.graphql(
        query=query,
        variables=dict(
            prefix=prefix,
            state=state,
            min_start_time=min_start_time,
            max_start_time=max_start_time,
        ),
    )["data"]
    return response


@task
def format_failed_runs(response: Dict, min_failures: int = 10):
    """
    Format data received from query as a discord message.
    If the query finds more than `min_failures`, adds a
    @here marker to the message.
    Args:
        response(dict): data received from a query to GraphQL
        API.
        min_failures(Optional, int): minimum number of failures
        to add the marker @here
    Returns:
        str: the message to post
    """
    base_msg = "Falhas na execução dos flows na ultima hora:\n"
    for flow in response["flow"]:
        flow_msg = f"""
        O Flow `{flow['name']}` registrado em `{flow['project']['name']}`
        teve as seguintes falhas:\n
        """
        if len(flow["flow_runs"]) >= min_failures:
            flow_msg += "---Critical---\n@here\n"
        for run in flow["flow_runs"]:
            flow_msg += f"""
            ---------------------------------
            run_name: {run['name']}
            scheduled_start_time: {run['scheduled_start_time']}
            label: {run['labels'][0]}
            error: {run['state_message']}
            ---------------------------------\n
            """
        base_msg += flow_msg
    return base_msg


@task
def post_to_discord_from_secret(message: str, webhook_secret_path: str):
    """Post message to a discord channel by fetching the webhook_url
    from a secret

    Args:
        message (str): message to post
        webhook_secret_path (str): secret path to get the url.
        Should be defined with a key `url`.
    """
    webhook_url = get_vault_secret(webhook_secret_path)["url"]
    return send_discord_message(message=message, webhook_url=webhook_url)
