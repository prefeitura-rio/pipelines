# -*- coding: utf-8 -*-
"""
Dumping data from URLs to class.
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.dump_db.tasks import (
    parse_comma_separated_string_to_list,
)
from pipelines.utils.dump_url.tasks import download_url, dump_files
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)

with Flow(
    name=utils_constants.FLOW_DUMP_URL_FORMACAO_NAME.value,
    code_owners=[
        "diego",
        "gabriel",
    ],
) as dump_url_flow:

    #####################################
    #
    # Parameters
    #
    #####################################

    # URL parameters
    url = Parameter("url")
    url_type = Parameter("url_type", default="direct", required=True)
    gsheets_sheet_order = Parameter("gsheets_sheet_order", default=0, required=False)
    gsheets_sheet_name = Parameter("gsheets_sheet_name", default=None, required=False)
    gsheets_sheet_range = Parameter("gsheets_sheet_range", default=None, required=False)

    # Table parameters
    partition_columns = Parameter("partition_columns", required=False, default="")

    # BigQuery parameters
    dataset_id = Parameter("dataset_id")
    table_id = Parameter("table_id")
    dump_mode = Parameter("dump_mode", default="overwrite")  # overwrite or append
    batch_data_type = Parameter("batch_data_type", default="csv")  # csv or parquet

    # JSON dataframe parameters
    dataframe_key_column = Parameter(
        "dataframe_key_column", default=None, required=False
    )
    build_json_dataframe = Parameter(
        "build_json_dataframe", default=False, required=False
    )

    #####################################
    #
    # Rename flow run
    #
    #####################################
    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    # Get current flow labels
    current_flow_labels = get_current_flow_labels()
    current_flow_labels.set_upstream(rename_flow_run)

    #####################################
    #
    # Tasks section #1 - Get data
    #
    #####################################
    DATA_PATH = "/tmp/dump_url/"
    DUMP_DATA_PATH = "/tmp/dump_url_chunks/"
    DATA_FNAME = DATA_PATH + "data.csv"
    DOWNLOAD_URL_TASK = download_url(
        url=url,
        fname=DATA_FNAME,
        url_type=url_type,
        gsheets_sheet_order=gsheets_sheet_order,
        gsheets_sheet_name=gsheets_sheet_name,
        gsheets_sheet_range=gsheets_sheet_range,
    )
    DOWNLOAD_URL_TASK.set_upstream(rename_flow_run)

    partition_columns = parse_comma_separated_string_to_list(text=partition_columns)

    DUMP_CHUNKS_TASK = dump_files(
        file_path=DATA_FNAME,
        partition_columns=partition_columns,
        save_path=DUMP_DATA_PATH,
        build_json_dataframe=build_json_dataframe,
        dataframe_key_column=dataframe_key_column,
    )
    DUMP_CHUNKS_TASK.set_upstream(DOWNLOAD_URL_TASK)

    #####################################
    #
    # Tasks section #2 - Create table
    #
    #####################################
    CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK = create_table_and_upload_to_gcs(
        data_path=DUMP_DATA_PATH,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
    )
    CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK.set_upstream(DOWNLOAD_URL_TASK)
    CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK.set_upstream(DUMP_CHUNKS_TASK)

dump_url_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_url_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
