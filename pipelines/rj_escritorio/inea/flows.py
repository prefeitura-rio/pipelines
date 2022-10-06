# -*- coding: utf-8 -*-
"""
Flows for INEA.
"""
from prefect import Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_escritorio.inea.tasks import (
    convert_vol_files,
    fetch_vol_files,
    upload_files_to_gcs,
)
from pipelines.utils.decorators import Flow

with Flow(
    "INEA: Teste",
    code_owners=[
        "gabriel",
    ],
) as inea_test_flow:
    date = Parameter("date")
    bucket_name = Parameter("bucket_name")
    prefix = Parameter("prefix")
    mode = Parameter("mode", default="prod", required=False)
    radar = Parameter("radar")
    product = Parameter("product")
    output_format = Parameter("output_format", default="NetCDF", required=False)
    convert_params = Parameter(
        "convert_params",
        default="-f=Whole -k=CFext -r=Short -p=Radar -M=All -z",
        required=False,
    )
    FETCH_TASK = fetch_vol_files(date=date)
    CONVERTED_FILES = convert_vol_files(
        output_format=output_format, convert_params=convert_params
    )
    CONVERTED_FILES.set_upstream(FETCH_TASK)  # pylint: disable=no-member
    upload_files_to_gcs(
        converted_files=CONVERTED_FILES,
        bucket_name=bucket_name,
        prefix=prefix,
        mode=mode,
        radar=radar,
        product=product,
    )


inea_test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_test_flow.run_config = LocalRun(labels=[constants.INEA_AGENT_LABEL.value])
