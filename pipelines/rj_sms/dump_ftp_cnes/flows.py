# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
CNES dumping flows
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_ftp_cnes.constants import constants as cnes_constants
from pipelines.rj_sms.utils import download_ftp, create_folders, unzip_file
from pipelines.rj_sms.dump_ftp_cnes.tasks import (
    check_newest_file_version,
    conform_csv_to_gcp,
    upload_multiple_tables_to_datalake,
    add_multiple_date_column,
)
from pipelines.rj_sms.dump_ftp_cnes.schedules import every_sunday_at_six_am

with Flow(
    name="SMS: Dump CNES - Captura de dados CNES", code_owners=["thiago"]
) as dump_cnes:
    # Parameters
    # Parameters for GCP
    dataset_id = cnes_constants.DATASET_ID.value
    tables_to_upload = cnes_constants.TABLES_TO_UPLOAD.value
    # Parameters for CNES
    ftp_server = cnes_constants.FTP_SERVER.value
    ftp_file_path = cnes_constants.FTP_FILE_PATH.value
    base_file = cnes_constants.BASE_FILE.value

    # Start run
    check_newest_file_version_task = check_newest_file_version(
        host=ftp_server,
        user="",
        password="",
        directory=ftp_file_path,
        file_name=base_file,
    )

    create_folders_task = create_folders()
    create_folders_task.set_upstream(check_newest_file_version_task)

    download_task = download_ftp(
        host=ftp_server,
        user="",
        password="",
        directory=ftp_file_path,
        file_name=check_newest_file_version_task["file"],
        output_path=create_folders_task["raw"],
    )
    download_task.set_upstream(create_folders_task)

    unzip_task = unzip_file(
        file_path=download_task, output_path=create_folders_task["raw"]
    )
    unzip_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(create_folders_task["raw"])
    conform_task.set_upstream(unzip_task)

    add_multiple_date_column_task = add_multiple_date_column(
        directory=create_folders_task["raw"],
        snapshot_date=check_newest_file_version_task["snapshot"],
        sep=";",
    )
    add_multiple_date_column_task.set_upstream(conform_task)

    upload_to_datalake_task = upload_multiple_tables_to_datalake(
        path_files=conform_task, dataset_id=dataset_id, dump_mode="overwrite"
    )
    upload_to_datalake_task.set_upstream(add_multiple_date_column_task)

dump_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_cnes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

dump_cnes.schedule = every_sunday_at_six_am
