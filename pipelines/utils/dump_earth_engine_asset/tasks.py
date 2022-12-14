# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa: E501
"""
Tasks for dumping data directly from BigQuery to GCS.
"""
from datetime import datetime
import json
from pathlib import Path
from time import sleep

from basedosdados.download.base import google_client
from basedosdados.upload.base import Base
import ee
from google.cloud import bigquery
from prefect import task

from pipelines.utils.utils import (
    determine_whether_to_execute_or_not,
    get_redis_client,
    get_vault_client,
    human_readable,
    list_blobs_with_prefix,
    log,
)


@task
def download_data_to_gcs(  # pylint: disable=R0912,R0913,R0914,R0915
    project_id: str = None,
    query: str = None,
    gcs_asset_path: str = None,
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
    location: str = "US",
):
    """
    Get data from BigQuery.
    """
    # Try to get project_id from environment variable
    if not project_id:
        log("Project ID was not provided, trying to get it from environment variable")
        try:
            bd_base = Base()
            project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
        except KeyError:
            pass
        if not project_id:
            raise ValueError(
                "project_id must be either provided or inferred from environment variables"
            )
        log(f"Project ID was inferred from environment variables: {project_id}")

    # If query is not a string, raise an error
    if not isinstance(query, str):
        raise ValueError("query must be either a string or a Jinja2 template")
    log(f"Query was provided: {query}")

    # Get billing project ID
    if not billing_project_id:
        log(
            "Billing project ID was not provided, trying to get it from environment variable"
        )
        try:
            bd_base = Base()
            billing_project_id = bd_base.config["gcloud-projects"][bd_project_mode][
                "name"
            ]
        except KeyError:
            pass
        if not billing_project_id:
            raise ValueError(
                "billing_project_id must be either provided or inferred from environment variables"
            )
        log(
            f"Billing project ID was inferred from environment variables: {billing_project_id}"
        )

    # pylint: disable=E1124
    client = google_client(project_id, billing_project_id, from_file=True, reauth=False)
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job = client["bigquery"].query(query, job_config=job_config)
    while not job.done():
        sleep(1)
    # pylint: disable=E1101
    table_size = job.total_bytes_processed
    log(f'Table size: {human_readable(table_size, unit="B", unit_divider=1024)}')

    # Get data
    log("Querying data from BigQuery")
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)
    # pylint: disable=protected-access
    dest_table = job._properties["configuration"]["query"]["destinationTable"]
    dest_project_id = dest_table["projectId"]
    dest_dataset_id = dest_table["datasetId"]
    dest_table_id = dest_table["tableId"]
    log(
        f"Query results were stored in {dest_project_id}.{dest_dataset_id}.{dest_table_id}"
    )

    blob_path = f"{gcs_asset_path}/data*.csv.gz"
    log(f"Loading data to {blob_path}")
    dataset_ref = bigquery.DatasetReference(dest_project_id, dest_dataset_id)
    table_ref = dataset_ref.table(dest_table_id)
    job_config = bigquery.job.ExtractJobConfig(compression="GZIP")
    extract_job = client["bigquery"].extract_table(
        table_ref,
        blob_path,
        location=location,
        job_config=job_config,
    )
    extract_job.result()
    log("Data was loaded successfully")

    # Get the BLOB we've just created and make it public
    blobs = list_blobs_with_prefix(project_id, blob_path)
    log(f"{blobs}")
    if not blobs:
        raise ValueError(f"No blob found at {blob_path}")

    return blob_path


@task
def get_project_id(
    project_id: str = None,
    bd_project_mode: str = "prod",
):
    """
    Get the project ID.
    """
    if project_id:
        return project_id
    log("Project ID was not provided, trying to get it from environment variable")
    try:
        bd_base = Base()
        project_id = bd_base.config["gcloud-projects"][bd_project_mode]["name"]
    except KeyError:
        pass
    if not project_id:
        raise ValueError(
            "project_id must be either provided or inferred from environment variables"
        )
    log(f"Project ID was inferred from environment variables: {project_id}")
    return project_id


@task(nout=2)
def trigger_cron_job(
    project_id: str,
    ee_asset_path: str,
    cron_expression: str,
):
    """
    Tells whether to trigger a cron job.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{ee_asset_path}"
    log(f"Checking if cron job should be triggered for {key}")
    val = redis_client.get(key)
    current_datetime = datetime.now()
    if val and val is dict and "last_trigger" in val:
        last_trigger = val["last_trigger"]
        log(f"Last trigger: {last_trigger}")
        if last_trigger:
            return determine_whether_to_execute_or_not(
                cron_expression, current_datetime, last_trigger
            )
    log(f"No last trigger found for {key}")
    return True, current_datetime


@task
def update_last_trigger(
    project_id: str,
    ee_asset_path: str,
    execution_time: datetime,
):
    """
    Update the last trigger.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{ee_asset_path}"
    redis_client.set(key, {"last_trigger": execution_time})


@task
def get_earth_engine_key_from_vault(
    vault_path_earth_engine_key: str,
):
    """
    Get earth engine service account key from vault.
    """
    log(
        f"Getting Earth Engine key from https://vault.dados.rio/ui/vault/secrets/secret/show/{vault_path_earth_engine_key}"
    )
    vault_client = get_vault_client()

    secret = vault_client.secrets.kv.read_secret_version(vault_path_earth_engine_key)[
        "data"
    ]["key"]

    service_account_secret_path = Path("/tmp/earth-engine/key.json")
    service_account_secret_path.parent.mkdir(parents=True, exist_ok=True)

    with open(service_account_secret_path, "w", encoding="utf-8") as f:
        json.dump(secret, f, ensure_ascii=False, indent=4)

    return service_account_secret_path


@task
def create_table_asset(
    service_account: str,
    service_account_secret_path: str,
    project_id: str,
    gcs_file_asset_path: str,
    ee_asset_path: str,
):
    """
    Create a table asset in Earth Engine.

    parameters:
        service_account
            Service account email in the format of earth-engine@<project-id>.iam.gserviceaccount.com
        service_account_secret_path
            Path to the .json file containing the service account secret.
        project_id:
            Earth Engine project ID.
        gcs_asset_path
            Path to the asset in Google Cloud Storage in the format of "gs://<project-id>/<some-folder>/file.csv"
        ee_asset_path
            Path that the asset will be created in Earth Engine in the format of
            projects/<project-id>/assets/<asset_name> or users/<user-id>/<asset_name>
    """

    credentials = ee.ServiceAccountCredentials(
        service_account, service_account_secret_path
    )
    ee.Initialize(credentials=credentials, project=project_id)

    params = {
        "name": ee_asset_path,
        "sources": [{"primaryPath": gcs_file_asset_path, "charset": "UTF-8"}],
    }

    request_id = ee.data.newTaskId(1)[0]
    task_status = ee.data.startTableIngestion(request_id=request_id, params=params)
    log(ee.data.getTaskStatus(task_status["id"]))
