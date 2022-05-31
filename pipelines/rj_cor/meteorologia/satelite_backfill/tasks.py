# -*- coding: utf-8 -*-
"""
Tasks for backfill pipeline.
"""

from os import remove
from pathlib import Path
from prefect import task
from prefect.triggers import all_finished


@task
def delete_files(
    filename: str,
    input_filename: str,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Deletar tanto os arquivos raw, quanto os temporários e de output
    """
    filename_temp = filename.replace("output", "temp").replace(".csv", ".tif")
    files = [filename, filename_temp, input_filename]

    [remove(f) for f in files]

    # get_dir = filename.split("input")[0]
    # print(f"Deleting files from {get_dir}")
    # [f.unlink() for f in Path(r"{}".format(get_dir)).glob("**/*") if f.is_file()]
