# -*- coding: utf-8 -*-
"""
Tasks for backfill pipeline.
"""

from pathlib import Path
from prefect import task
from prefect.triggers import all_finished


@task
def delete_files(
    filename: str,
    wait=None,  # pylint: disable=unused-argument
):
    get_dir = filename.split("input")[0]
    print(f"Deleting files from {get_dir}")
    [f.unlink() for f in Path(r"{}".format(get_dir)).glob("**/*") if f.is_file()]
