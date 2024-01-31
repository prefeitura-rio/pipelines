# -*- coding: utf-8 -*-
from typing import Dict, List

import pendulum
from prefect import task
from prefect.client import Client

from pipelines.utils.utils import log, get_redis_client


def query_active_flow_names(prefix, prefect_client=None):
    query = """
query ($prefix: String, $offset: Int){
    flow(
        where: {
            name: {_like: $prefix},
            archived: {_eq: false},
            project: {name:{_eq:"main"}}
        }
        offset: $offset
    ){
        name
        version
    }
}
"""
    if not prefect_client:
        prefect_client = Client()
    variables = {"prefix": prefix, "offset": 0}
    flow_names = []
    response = prefect_client.graphql(query=query, variables=variables)["data"]
    for flow in response["flow"]:
        flow_names.append(flow["name"])
    flow_names = list(set(flow_names))
    return flow_names


@task
def get_prefect_client():
    return Client()


@task
def get_active_flow_names(prefix="%SMTR%"):
    flow_names = query_active_flow_names(prefix=prefix)
    log(f"Got flow_names\n{flow_names[:10]}\n...\n{flow_names[-10:-1]}")
    return flow_names


@task
def query_archived_scheduled_runs(flow_name, prefect_client=None):
    """
    Queries the graphql API for scheduled flow_runs of
    archived versions of <flow_name>

    Args:
        flow_name (str): flow name
    """
    query = """
query($flow_name: String, $offset: Int){
    flow(
        where:{
            name: {_eq:$flow_name},
            archived: {_eq:true},
            project: {name:{_eq:"main"}}
        }
        offset: $offset
        order_by: {version:desc}
    ){
        name
        version
        flow_runs(
            where:{
                state: {_eq: "Scheduled"}
            }
            order_by: {version:desc}
        ){
            id
            scheduled_start_time
        }
    }
}
"""
    if not prefect_client:
        prefect_client = Client()

    variables = {"flow_name": flow_name, "offset": 0}
    archived_flow_runs = []
    response = prefect_client.graphql(query=query, variables=variables)["data"]

    for flow in response["flow"]:
        for flow_run in flow["flow_runs"]:
            if flow["flow_runs"]:
                archived_flow_runs.append(flow_run)
            log(
                f"Got flow_run {flow_run['id']}, scheduled: {flow_run['scheduled_start_time']}"
            )
    # while len(response):
    #     if len(response["flow"]["flow_runs"]):

    #     variables["offset"] += len(response)
    #     response = prefect_client.graphql(query=query, variables=variables)
    if archived_flow_runs:
        log(f"O Flow {flow_name} possui runs a serem canceladas")
    return archived_flow_runs


@task
def cancel_flow_runs(flow_runs: List[Dict[str, str]], client: Client = None) -> None:
    """
    Cancels a flow run from the API.
    """
    if not flow_runs:
        log("No flow runs to cancel")
        return
    flow_run_ids = [flow_run["id"] for flow_run in flow_runs]
    log(f">>>>>>>>>> Cancelling flow runs\n{flow_run_ids}")
    if not client:
        client = Client()

    query = """
        mutation($flow_run_id: UUID!) {
            cancel_flow_run (
                input: {
                    flow_run_id: $flow_run_id
                }
            ) {
                state
            }
        }
    """
    for flow_run_id in flow_run_ids:
        try:
            response = client.graphql(
                query=query, variables=dict(flow_run_id=flow_run_id)
            )
            state: str = response["data"]["cancel_flow_run"]["state"]
            log(f">>>>>>>>>> Flow run {flow_run_id} is now {state}")
        except Exception:
            log(f"Flow_run {flow_run_id} could not be cancelled")
