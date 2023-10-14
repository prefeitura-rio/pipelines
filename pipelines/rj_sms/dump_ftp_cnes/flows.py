# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
TPC inventory dumping flows
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from pipelines.rj_sms.dump_ftp_cnes.constants import constants as cnes_constants
from pipelines.rj_sms.utils import (
    list_files_ftp,
    download_ftp,
    create_folders,
    unzip_file,
    add_load_date_column,
    create_partitions,
    upload_to_datalake,
)
from pipelines.rj_sms.dump_ftp_cnes.tasks import (
    check_newest_file_version,
    conform_csv_to_gcp,
    multiple_tables_upload_to_datalake
)
from pipelines.rj_sms.dump_azureblob_estoque_tpc.scheduler import every_day_at_six_am

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
        file_name=base_file)
    
    create_folders_task = create_folders()
    create_folders_task.set_upstream(check_newest_file_version_task)

    download_task = download_ftp(
        host=ftp_server,
        user="",
        password="",
        directory=ftp_file_path,
        file_name=check_newest_file_version_task,
        output_path=create_folders_task["raw"]
    )
    # download_task.set_upstream(create_folders_task)
    # download_task = "/home/thiagotrabach/projects/prefeitura/pipelines/pipelines/rj_sms/data/raw/BASE_DE_DADOS_CNES_202308.ZIP"
    # output_path = "/home/thiagotrabach/projects/prefeitura/pipelines/pipelines/rj_sms/data/raw"
    # files = ["/home/thiagotrabach/projects/prefeitura/pipelines/pipelines/rj_sms/data/raw/rlAdmGerenciaCnes202307.csv",
    #         "/home/thiagotrabach/projects/prefeitura/pipelines/pipelines/rj_sms/data/raw/rlAtividadeObrigatoria202308.csv",
    #         "/home/thiagotrabach/projects/prefeitura/pipelines/pipelines/rj_sms/data/raw/rlEquipeAldeia202308.csv"]

    unzip_task = unzip_file(
        file_path=download_task,
        output_path=create_folders_task["raw"] #output_path
    )
    unzip_task.set_upstream(download_task)

    conform_task = conform_csv_to_gcp(create_folders_task["raw"])
    conform_task.set_upstream(unzip_task)

    #add_load_date_column_task = add_load_date_column(
    #    input_path=download_task, sep=";"
    #)
    #add_load_date_column_task.set_upstream(conform_task)

    upload_to_datalake_task = multiple_tables_upload_to_datalake(
        path_files=conform_task,
        dataset_id=dataset_id,
        dump_mode="overwrite")
    upload_to_datalake_task.set_upstream(conform_task)

dump_cnes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_cnes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)

#dump_cnes.schedule = every_day_at_six_am