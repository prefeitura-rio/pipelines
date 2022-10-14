# -*- coding: utf-8 -*-
"""
Flows for INEA.
"""
from prefect import Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.inea.tasks import (
    convert_vol_file,
    execute_shell_command,
    fetch_vol_file,
    list_vol_files,
    upload_file_to_gcs,
)
from pipelines.utils.decorators import Flow

with Flow(
    "INEA: Teste",
    code_owners=[
        "gabriel",
    ],
) as inea_test_flow:
    date = Parameter("date", default=None, required=False)
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
    greater_than = Parameter("greater_than", default=None, required=False)
    remote_files, output_directory = list_vol_files(
        date=date, greater_than=greater_than
    )
    downloaded_files = fetch_vol_file.map(
        remote_file=remote_files, output_directory=unmapped(output_directory)
    )
    converted_files = convert_vol_file.map(
        downloaded_file=downloaded_files,
        output_format=unmapped(output_format),
        convert_params=unmapped(convert_params),
    )
    upload_file_to_gcs.map(
        converted_file=converted_files,
        bucket_name=unmapped(bucket_name),
        prefix=unmapped(prefix),
        mode=unmapped(mode),
        radar=unmapped(radar),
        product=unmapped(product),
    )


inea_test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_test_flow.run_config = LocalRun(labels=[constants.INEA_AGENT_LABEL.value])


with Flow(
    "INEA: Executar comando no terminal",
    code_owners=[
        "gabriel",
    ],
) as inea_execute_shell_command_flow:
    command = Parameter("command")
    execute_shell_command(command=command)

inea_execute_shell_command_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_execute_shell_command_flow.run_config = LocalRun(
    labels=[constants.INEA_AGENT_LABEL.value]
)

with Flow(
    "INEA: Fazer upload de arquivo para o GCS",
    code_owners=[
        "gabriel",
    ],
) as inea_upload_file_to_gcs_flow:
    filename = Parameter("filename")
    bucket_name = Parameter("bucket_name")
    prefix = Parameter("prefix")
    mode = Parameter("mode", default="prod", required=False)
    upload_file_to_gcs(
        converted_file=filename,
        bucket_name=bucket_name,
        prefix=prefix,
        radar=None,
        product=None,
        mode="prod",
        task_mode="raw",
        unlink=False,
    )

inea_upload_file_to_gcs_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_upload_file_to_gcs_flow.run_config = LocalRun(
    labels=[constants.INEA_AGENT_LABEL.value]
)
