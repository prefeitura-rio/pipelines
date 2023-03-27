# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0302
"""
General utils for setting rain dashboard using radar data.
"""
from google.cloud import storage


def download_blob(bucket_name, source_blob_name, destination_file_name) -> None:
    """
    Downloads a blob mencioned on source_blob_name from bucket_name
    and save it on destination_file_name.
    """

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Blob {} downloaded to file path {}. successfully ".format(
            source_blob_name, destination_file_name
        )
    )
