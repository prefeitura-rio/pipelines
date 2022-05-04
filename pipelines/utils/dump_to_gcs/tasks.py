# -*- coding: utf-8 -*-
from time import sleep
from typing import Union

import basedosdados as bd
from basedosdados.download.base import google_client
from basedosdados.upload.base import Base
from google.cloud import bigquery
import jinja2
import pandas as pd
from prefect import task

from pipelines.utils.utils import log


@task
def download_data_to_gcs(
    project_id: str = None,
    dataset_id: str = None,
    table_id: str = None,
    query: Union[str, jinja2.Template] = None,
    query_params: dict = None,
    mode: str = "prod",
    billing_project_id: str = None,
    location: str = "southamerica-east1",
):
    """
    Get data from BigQuery.
    """
    # Either (dataset_id and table_id) or query must be provided
    if not (dataset_id and table_id) and not query:
        raise ValueError("Either (dataset_id and table_id) or query must be provided")

    # Try to get project_id from environment variable
    if not project_id:
        log("Project ID was not provided, trying to get it from environment variable")
        try:
            bd_base = Base()
            project_id = bd_base.config["gcloud-projects"][mode]["name"]
        except KeyError:
            pass
        if not project_id:
            raise ValueError(
                "project_id must be either provided or inferred from environment variables"
            )
        log(f"Project ID was inferred from environment variables: {project_id}")

    # If dataset_id and table_id are provided, build query from it
    if dataset_id and table_id:
        query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`"
        log(f"Query was inferred from dataset_id and table_id: {query}")

    # If query is provided, use it!
    # If it's a template, we must render it.
    if not query_params:
        query_params = {}
    if isinstance(query, jinja2.Template):
        try:
            query = query.render(
                {
                    "project_id": project_id,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    **query_params,
                }
            )
        except jinja2.TemplateError as e:
            raise ValueError(f"Error rendering query: {e}")
        log(f"Query was rendered: {query}")

    # If query is not a string, raise an error
    if not isinstance(query, str):
        raise ValueError("query must be either a string or a Jinja2 template")
    else:
        log(f"Query was provided: {query}")

    # Get billing project ID
    if not billing_project_id:
        log(
            "Billing project ID was not provided, trying to get it from environment variable"
        )
        try:
            bd_base = Base()
            billing_project_id = bd_base.config["gcloud-projects"][mode]["name"]
        except KeyError:
            pass
        if not billing_project_id:
            raise ValueError(
                "billing_project_id must be either provided or inferred from environment variables"
            )
        log(
            f"Billing project ID was inferred from environment variables: {billing_project_id}"
        )

    # Get data
    log("Querying data from BigQuery")
    client = google_client(project_id, billing_project_id, from_file=True, reauth=False)
    job = client["bigquery"].query(query)
    while not job.done():
        sleep(1)
    dest_table = job._properties["configuration"]["query"]["destinationTable"]
    dest_project_id = dest_table["projectId"]
    dest_dataset_id = dest_table["datasetId"]
    dest_table_id = dest_table["tableId"]
    log(
        f"Query results were stored in {dest_project_id}.{dest_dataset_id}.{dest_table_id}"
    )

    blob_path = f"gs://datario/share/{dataset_id}/{table_id}/data.tar.gz"
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
