# -*- coding: utf-8 -*-
"""
Tasks for dumping data directly from BigQuery to GCS.
"""
from datetime import datetime
from time import sleep
from typing import Union

from basedosdados.download.base import google_client
from basedosdados.upload.base import Base
from google.cloud import bigquery
import jinja2
from prefect import task

from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.utils import (
    determine_whether_to_execute_or_not,
    get_redis_client,
    human_readable,
    list_blobs_with_prefix,
    log,
)


@task
def download_data_to_gcs(  # pylint: disable=R0912,R0913,R0914,R0915
    dataset_id: str,
    table_id: str,
    project_id: str = None,
    query: Union[str, jinja2.Template] = None,
    jinja_query_params: dict = None,
    bd_project_mode: str = "prod",
    billing_project_id: str = None,
    location: str = "US",
    maximum_bytes_processed: float = dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
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

    # Asserts that dataset_id and table_id are provided
    if not dataset_id or not table_id:
        raise ValueError("dataset_id and table_id must be provided")

    # If query is not provided, build query from it
    if not query:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
        log(f"Query was inferred from dataset_id and table_id: {query}")

    # If query is provided, use it!
    # If it's a template, we must render it.
    if not query:
        if not jinja_query_params:
            jinja_query_params = {}
        if isinstance(query, jinja2.Template):
            try:
                query = query.render(
                    {
                        "project_id": project_id,
                        "dataset_id": dataset_id,
                        "table_id": table_id,
                        **jinja_query_params,
                    }
                )
            except jinja2.TemplateError as exc:
                raise ValueError(f"Error rendering query: {exc}") from exc
            log(f"Query was rendered: {query}")

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

    # Checking if data exceeds the maximum allowed size
    log("Checking if data exceeds the maximum allowed size")
    # pylint: disable=E1124
    client = google_client(project_id, billing_project_id, from_file=True, reauth=False)
    job_config = bigquery.QueryJobConfig()
    job_config.dry_run = True
    job = client["bigquery"].query(query, job_config=job_config)
    while not job.done():
        sleep(1)
    table_size = job.total_bytes_processed
    log(f'Table size: {human_readable(table_size, unit="B", unit_divider=1024)}')
    if table_size > maximum_bytes_processed:
        max_allowed_size = human_readable(
            maximum_bytes_processed,
            unit="B",
            unit_divider=1024,
        )
        raise ValueError(
            f"Table size exceeds the maximum allowed size: {max_allowed_size}"
        )

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

    blob_path = f"gs://datario/share/{dataset_id}/{table_id}/data*.csv.gz"
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
    blobs = list_blobs_with_prefix("datario", f"share/{dataset_id}/{table_id}/")
    if not blobs:
        raise ValueError(f"No blob found at {blob_path}")
    for blob in blobs:
        log(f"Blob found at {blob.name}")
        blob.make_public()
        log("Blob was made public")


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
    dataset_id: str,
    table_id: str,
    cron_expression: str,
):
    """
    Tells whether to trigger a cron job.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
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
    dataset_id: str,
    table_id: str,
    execution_time: datetime,
):
    """
    Update the last trigger.
    """
    redis_client = get_redis_client()
    key = f"{project_id}__{dataset_id}__{table_id}"
    redis_client.set(key, {"last_trigger": execution_time})
