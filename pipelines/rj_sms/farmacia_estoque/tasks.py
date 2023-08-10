from prefect import task
from azure.storage.blob import BlobServiceClient
from pipelines.utils.utils import log



@task
def download_blob(connection_string, container_name, blob_name, destination_file_path):
    """
    Download a blob from Azure Blob Storage to a local file.

    :param connection_string: Azure Blob Storage connection string
    :param container_name: Name of the container where the blob is located
    :param blob_name: Name of the blob to download
    :param destination_file_path: Local file path to save the downloaded blob
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

    with open(destination_file_path, "wb") as blob_file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(blob_file)

    log(f"Blob '{blob_name}' downloaded to '{destination_file_path}'.")

@task
def list_blobs(connection_string, container_name):
    """
    List blobs inside a container in Azure Blob Storage.

    :param connection_string: Azure Blob Storage connection string
    :param container_name: Name of the container to list blobs from
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs()
    
    log(f"Blobs in container '{container_name}':")
    for blob in blob_list:
        print(blob.name)