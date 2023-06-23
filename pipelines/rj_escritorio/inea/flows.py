# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for INEA.
"""
from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import LocalRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.inea.schedules import every_5_minutes
from pipelines.rj_escritorio.inea.tasks import (
    convert_vol_file,
    execute_shell_command,
    fetch_vol_file,
    list_vol_files,
    upload_file_to_gcs,
)
from pipelines.utils.decorators import Flow

with Flow(
    "INEA: Captura dados de radar",
    code_owners=[
        "paty",
    ],
    skip_if_running=True,
) as inea_radar_flow:
    date = Parameter("date", default=None, required=False)
    bucket_name = Parameter("bucket_name", default="rj-escritorio-dev", required=False)
    prefix = Parameter(
        "prefix", default="raw/meio_ambiente_clima/inea_radar_hdf5", required=False
    )
    mode = Parameter("mode", default="prod", required=False)
    radar = Parameter("radar", default="gua", required=False)
    product = Parameter("product", default="ppi", required=False)
    output_format = Parameter("output_format", default="HDF5", required=False)
    convert_params = Parameter(
        "convert_params",
        default="-k=ODIM2.1 -M=All",
        required=False,
    )
    greater_than = Parameter("greater_than", default=None, required=False)
    vols_remote_directory = Parameter(
        "vols_remote_directory", default="/var/opt/edge/vols", required=False
    )
    get_only_last_file = Parameter("get_only_last_file", default=True, required=False)

    remote_files, output_directory = list_vol_files(
        date=date,
        greater_than=greater_than,
        bucket_name=bucket_name,
        prefix=prefix,
        radar=radar,
        product=product,
        get_only_last_file=get_only_last_file,
        mode=mode,
        output_format=output_format,
        vols_remote_directory=vols_remote_directory,
    )
    downloaded_files = fetch_vol_file.map(
        remote_file=remote_files,
        radar=radar,
        output_directory=unmapped(output_directory),
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


inea_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_radar_flow.run_config = LocalRun(labels=[constants.INEA_AGENT_LABEL.value])
inea_radar_flow.schedule = every_5_minutes

inea_backfill_radar_flow = deepcopy(inea_radar_flow)
inea_backfill_radar_flow.name = "INEA: Captura dados de radar (backfill)"
inea_backfill_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_backfill_radar_flow.run_config = LocalRun(
    labels=[constants.INEA_AGENT_LABEL.value]
)
inea_backfill_radar_flow.schedule = None

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
