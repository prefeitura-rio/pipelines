# -*- coding: utf-8 -*-
"""
Dumping  data from INEA FTP to BigQuery
"""
# pylint: disable=E1101,C0103

from copy import deepcopy

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.dump_ftp_inea.tasks import (
    get_ftp_client,
    get_files_to_download,
    download_files,
    upload_file_to_gcs,
)
from pipelines.rj_escritorio.dump_ftp_inea.schedules import (
    every_5_minutes,
    every_5_minutes_mac,
)
from pipelines.utils.decorators import Flow


with Flow(
    "INEA: Captura FTP dados de radar (Guaratiba)",
    code_owners=[
        "paty",
    ],
) as inea_ftp_radar_flow:
    bucket_name = Parameter("bucket_name", default="rj-escritorio-dev", required=False)
    prefix = Parameter(
        "prefix", default="raw/meio_ambiente_clima/inea_radar_hdf5", required=False
    )
    mode = Parameter("mode", default="prod", required=False)
    radar = Parameter("radar", default="mac", required=False)
    product = Parameter("product", default="ppi", required=False)

    client = get_ftp_client()

    files_to_download = get_files_to_download(
        client=client,
        radar=radar,
    )

    files_to_upload = download_files(
        client=client,
        files=files_to_download,
        radar=radar,
    )

    upload_file_to_gcs.map(
        file_to_upload=files_to_upload,
        bucket_name=unmapped(bucket_name),
        prefix=unmapped(prefix),
        mode=unmapped(mode),
        radar=unmapped(radar),
        product=unmapped(product),
    )


inea_ftp_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
inea_ftp_radar_flow.schedule = every_5_minutes

inea_ftp_radar_flow_mac = deepcopy(inea_ftp_radar_flow)
inea_ftp_radar_flow_mac.name = "INEA: Captura FTP dados de radar (Macaé)"
inea_ftp_radar_flow_mac.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inea_ftp_radar_flow_mac.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
inea_ftp_radar_flow_mac.schedule = every_5_minutes_mac
