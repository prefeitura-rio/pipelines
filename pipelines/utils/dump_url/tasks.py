# -*- coding: utf-8 -*-
"""
General purpose tasks for dumping data from URLs.
"""
from datetime import timedelta
from pathlib import Path

from prefect import task
import requests

from pipelines.constants import constants


@task(
    checkpoint=False,
    max_retries=constants.TASK_MAX_RETRIES.value,
    retry_delay=timedelta(seconds=constants.TASK_RETRY_DELAY.value),
)
def download_url(url: str, fname: str) -> None:
    """
    Downloads a file from a URL and saves it to a local file.
    Try to do it without using lots of RAM.

    Args:
        url: URL to download from.
        fname: Name of the file to save to.

    Returns:
        None.
    """
    filepath = Path(fname)
    filepath.mkdir(parents=True, exist_ok=True)
    r = requests.get(url, stream=True)
    with open(fname, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
                f.flush()
