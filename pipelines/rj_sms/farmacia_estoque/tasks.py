# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log, get_vault_secret
from azure.storage.blob import BlobServiceClient
from datetime import timezone


@task
def download_azure_blob(
    container_name, blob_name, destination_folder_path
):
    """
    Download a blob from Azure Blob Storage to a local file.

    :param connection_string: Azure Blob Storage connection string
    :param container_name: Name of the container where the blob is located
    :param blob_name: Name of the blob to download
    :param destination_folder_path: Local folder path to save the downloaded blob
    """
    connection_string = get_vault_secret(secret_path="estoque_tpc")["data"]["connection_string"]

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    destination_file_path = f"{destination_folder_path}/{blob_name}"

    with open(destination_file_path, "wb") as blob_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(blob_file)

    log(f"Blob '{blob_name}' downloaded to '{destination_file_path}'.")


@task
def list_blobs_after_time(connection_string, container_name, after_time) -> list:
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blobs = container_client.list_blobs()

    filtered_blobs = []
    after_time = after_time.replace(tzinfo=timezone.utc)

    for blob in blobs:
        if blob.last_modified >= after_time:
            filtered_blobs.append(blob.name)

    log(f"Blobs created or updated after the specified time: {filtered_blobs}")

    return filtered_blobs
