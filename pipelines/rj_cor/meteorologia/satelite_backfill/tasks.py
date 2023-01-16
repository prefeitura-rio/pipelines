# -*- coding: utf-8 -*-
"""
Tasks for backfill pipeline.
"""

import os
import shutil
from prefect import task


@task
def delete_files(
    mode_redis: str,
    wait=None,  # pylint: disable=unused-argument
):
    """
    Deletar tanto os arquivos raw, quanto os temporários e de output
    """
    path = f"/home/patricia/Documentos/escritorio_dados/prefeitura-rio/pipelines/\
        {mode_redis}/data/satelite/"
    if os.path.exists(path):
        shutil.rmtree(path)

    # filename_temp = filename.replace("output", "temp").replace(".csv", ".tif")
    # files = [filename, filename_temp, input_filename]

    # [remove(f) for f in files]

    # get_dir = filename.split("input")[0]
    # print(f"Deleting files from {get_dir}")
    # [f.unlink() for f in Path(r"{}".format(get_dir)).glob("**/*") if f.is_file()]
