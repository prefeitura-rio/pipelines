# -*- coding: utf-8 -*-
from typing import Dict, List
import traceback
from datetime import datetime

from prefect import task
from prefect.client import Client

from pipelines.utils.utils import log, get_vault_secret

import requests


@task
def query_active_flow_names(prefix="%SMTR%", prefect_client=None):
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
    # flow_names = []
    response = prefect_client.graphql(query=query, variables=variables)["data"]
    active_flows = []
    for flow in response["flow"]:
        active_flows.append((flow["name"], flow["version"]))
        # flow_names.append(flow["name"])
    # flow_names = list(set(flow_names))
    active_flows = list(set(active_flows))
    return active_flows


@task
def query_not_active_flows(flows, prefect_client=None):
    """
    Queries the graphql API for scheduled flow_runs of
    archived versions of <flow_name>

    Args:
        flow_name (str): flow name
    """
    flow_name, last_version = flows
    now = datetime.now().isoformat()
    query = """
query($flow_name: String, $last_version: Int, $now: timestamptz!, $offset: Int){
    flow(
        where:{
            name: {_eq:$flow_name},
            version: {_lt:$last_version}
            project: {name:{_eq:"main"}}
        }
        offset: $offset
        order_by: {version:desc}
    ){
        id
        name
        version
        flow_runs(
            where:{
                scheduled_start_time: {_gte: $now},
                state: {_nin: ["Cancelled"]}
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

    variables = {
        "flow_name": flow_name,
        "last_version": last_version,
        "now": now,
        "offset": 0,
    }
    archived_flows = []
    response = prefect_client.graphql(query=query, variables=variables)["data"]
    # log(response)
    for flow in response["flow"]:
        if flow["flow_runs"]:
            try:
                archived_flows.append(
                    {
                        "id": flow["id"],
                        "name": flow["name"],
                        "version": flow["version"],
                        "count": len(flow["flow_runs"]),
                    }
                )
            except Exception:
                log(flow)

    return archived_flows


def send_cancelled_run_on_discord(flows, webhook_url):
    message = f"""
Os Flows de nome {flows[0]['name']} tiveram as seguintes versões arquivadas:
Link para as versões:\n
"""
    for flow in flows:
        message.append(
            f"Versão {flow['version']}: https://prefect.dados.rio/default/flow-run/{flow['id']}"
        )

    r = requests.post(
        webhook_url,
        data={"content": message},
    )

    log(r.status_code)
    log(r.text)


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

    if archived_flow_runs:
        log(f"O Flow {flow_name} possui runs a serem canceladas")
    return {"flow_name": flow_name, "flow_runs": archived_flow_runs}


@task
def cancel_flows(flows, prefect_client: Client = None) -> None:
    """
    Cancels a flow run from the API.
    """
    if not flows:
        # log(f"O flow {flow_runs['flow_name']} não possui runs para cancelar")
        return
    log(">>>>>>>>>> Cancelling flows")

    if not prefect_client:
        prefect_client = Client()

    query = """
        mutation($flow_id: UUID!) {
            archive_flow (
                input: {
                    flow_id: $flow_id
                }
            ) {
                success
            }
        }
    """
    cancelled_flows = []

    for flow in flows:
        try:
            response = prefect_client.graphql(
                query=query, variables=dict(flow_id=flow["id"])
            )
            # state: str = response["data"]["cancel_flow_run"]["state"]
            log(response)
            log(f">>>>>>>>>> Flow run {flow['id']} arquivada")
            cancelled_flows.append(flow)
        except Exception:
            log(traceback.format_exc())
            log(f"Flow {flow['id']} could not be cancelled")

    # Notify cancellation

    try:
        url = get_vault_secret("cancelled_runs_webhook")["url"]
        send_cancelled_run_on_discord(cancelled_flows, flows, webhook_url=url)
    except Exception:
        log(traceback.format_exc())
        log("Could not get a webhook to send messages to")
