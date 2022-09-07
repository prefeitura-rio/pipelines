# -*- coding: utf-8 -*-
"""
General purpose functions for the meteorologia/refletividade_horizontal_radar project
"""
from google.cloud import storage


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    with open(source_file_name, "rb") as file:
        blob.upload_from_file(file)
