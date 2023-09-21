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
    get_files_datalake,
    get_files_to_download,
    download_files,
    upload_file_to_gcs,
)
from pipelines.rj_escritorio.dump_ftp_inea.schedules import (
    every_5_minutes,
    every_5_minutes_mac,
)
from pipelines.rj_cor.tasks import (
    get_on_redis,
    save_on_redis,
)
from pipelines.utils.decorators import Flow


with Flow(
    "INEA: Captura FTP dados de radar (Guaratiba)",
    code_owners=[
        "paty",
    ],
) as inea_ftp_radar_flow:
    bucket_name = Parameter("bucket_name", default="rj-escritorio-dev", required=False)
    date = Parameter("date", default=None, required=False)
    get_only_last_file = Parameter("get_only_last_file", default=True, required=False)
    greater_than = Parameter("greater_than", default=None, required=False)
    prefix = Parameter(
        "prefix", default="raw/meio_ambiente_clima/inea_radar_hdf5", required=False
    )
    mode = Parameter("mode", default="prod", required=False)
    radar = Parameter("radar", default="mac", required=False)
    product = Parameter("product", default="ppi", required=False)

    client = get_ftp_client()

    redis_files = get_on_redis(
        dataset_id="meio_ambiente_clima", table_id=radar, mode=mode
    )

    datalake_files = get_files_datalake(
        bucket_name=bucket_name,
        prefix=prefix,
        radar=radar,
        product=product,
        date=date,
        mode=mode,
    )

    files_to_download = get_files_to_download(
        client=client,
        radar=radar,
        redis_files=redis_files,
        datalake_files=datalake_files,
        get_only_last_file=get_only_last_file,
    )

    files_to_upload = download_files(
        client=client,
        files=files_to_download,
        radar=radar,
    )

    upload_files = upload_file_to_gcs.map(
        file_to_upload=files_to_upload,
        bucket_name=unmapped(bucket_name),
        prefix=unmapped(prefix),
        mode=unmapped(mode),
        radar=unmapped(radar),
        product=unmapped(product),
    )

    save_on_redis(
        dataset_id="meio_ambiente_clima",
        table_id=radar,
        mode=mode,
        files=files_to_upload,
        wait=upload_files,
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
