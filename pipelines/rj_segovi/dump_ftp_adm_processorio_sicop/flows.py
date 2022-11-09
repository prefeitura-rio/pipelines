# -*- coding: utf-8 -*-
"""
Dumping data from SICOP FTP to BigQuery 
"""
# pylint: disable=E1101

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_segovi.dump_ftp_adm_processorio_sicop.tasks import (
    get_download_files,
    download_files,
    parse_save_dataframe,
)
from pipelines.rj_segovi.dump_ftp_adm_processorio_sicop.schedules import (
    every_week_schedule,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)
from pipelines.utils.utils import set_default_parameters

with Flow(
    name="SEGOVI: Processo.rio-SICOP - Ingerir tabelas de FTP",
    code_owners=[
        "diego",
    ],
) as dump_ftp_sicop:
    pattern = Parameter(
        "pattern", default="ARQ2001", required=True
    )  # ARQ2001 or ARQ2296
    dataset_id = Parameter("dataset_id", default="adm_processorio_sicop", required=True)
    table_id = Parameter("table_id", default="arq2001", required=True)

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id, wait=table_id
    )

    client, files_to_download, widths_columns = get_download_files(
        pattern=pattern,
        dataset_id=dataset_id,
        table_id=table_id,
        date_format='"%Y-%m-%d"',
    )

    files_to_parse = download_files(
        client=client, files=files_to_download, save_path="./ftp/raw/"
    )
    files_to_parse.set_upstream(client)

    save_path = parse_save_dataframe(
        files=files_to_parse,
        save_path="./data/",
        widths_columns=widths_columns,
    )
    parse_save_dataframe.set_upstream(files_to_parse)

    create_table_and_upload_to_gcs(
        data_path=save_path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
    )
    create_table_and_upload_to_gcs.set_upstream(save_path)

dump_ftp_sicop.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_ftp_sicop.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SEGOVI_AGENT_LABEL.value,
    ],
)

dump_ftp_sicop_default_parameters = {
    "pattern": "ARQ2001",
    "dataset_id": "adm_processorio_sicop",
    "table_id": "arq2001",
}

dump_ftp_sicop = set_default_parameters(
    dump_ftp_sicop, default_parameters=dump_ftp_sicop_default_parameters
)

dump_ftp_sicop.schedule = every_week_schedule
