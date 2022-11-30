# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from prefect import task
from typing import Dict

# from pathlib import Path
# import zipfile
# import shutil
# import traceback
# import pandas as pd
# from functools import reduce
import basedosdados as bd

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.rj_smtr.utils import (
    bq_project,
)

from pipelines.utils.utils import (
    # get_storage_blobs,
    log,
)


@task
def get_gtfs_version(run_date: str) -> str:
    """
    Get GTFS version
    """

    query = f"""
    SELECT
        timestamp_captura
    FROM
        rj-smtr-dev.gtfs.feed_info
    WHERE
        date("{run_date}") BETWEEN feed_start_date AND feed_end_date
    """

    results = bd.read_sql(query=query, billing_project_id=bq_project())
    if len(results) > 0:
        gtfs_version = max(results.values)[0]
        log(f"Got gtfs_version {gtfs_version} for run_date {run_date}")
        return gtfs_version

    else:
        log(
            f"""No GTFS version found for run_date: {run_date}.
            Searching for last GTFS version available."""
        )
        query = """
        SELECT
            timestamp_captura
        FROM
            rj-smtr-dev.gtfs.feed_info
        WHERE
            timestamp_captura = (select max(timestamp_captura) from rj-smtr-dev.gtfs.feed_info)
        """
        gtfs_version = bd.read_sql(query=query, billing_project_id=bq_project()).values[
            0
        ]
        log("Got gtfs_version: {}".format(gtfs_version[0]))
        return gtfs_version
