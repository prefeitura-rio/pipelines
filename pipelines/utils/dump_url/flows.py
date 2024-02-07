# -*- coding: utf-8 -*-
# pylint: disable=E1101
"""
Dumping  data from URLs
"""
from datetime import timedelta

from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_db.tasks import (
    parse_comma_separated_string_to_list,
)
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.dump_url.tasks import download_url, dump_files
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    get_current_flow_labels,
    rename_current_flow_run_dataset_table,
    create_table_and_upload_to_gcs,
)

with Flow(
    name=utils_constants.FLOW_DUMP_URL_NAME.value,
    code_owners=[
        "diego",
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
    encoding = Parameter("encoding", required=False, default="utf-8")
    on_bad_lines = Parameter("on_bad_lines", required=False, default="error")
    separator = Parameter("separator", required=False, default=",")

    # Materialization parameters
    materialize_after_dump = Parameter(
        "materialize_after_dump", default=False, required=False
    )
    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    materialize_to_datario = Parameter(
        "materialize_to_datario", default=False, required=False
    )

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    dump_to_gcs = Parameter("dump_to_gcs", default=False, required=False)
    maximum_bytes_processed = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

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
    biglake_table = Parameter("biglake_table", default=False, required=False)
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
        encoding=encoding,
        on_bad_lines=on_bad_lines,
        separator=separator,
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
        biglake_table=biglake_table,
        dump_mode=dump_mode,
    )
    CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK.set_upstream(DUMP_CHUNKS_TASK)

    #####################################
    #
    # Tasks section #3 - Materialize
    #
    #####################################
    with case(materialize_after_dump, True):
        # Trigger DBT flow run
        materialization_flow = create_flow_run(
            flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            parameters={
                "dataset_id": dataset_id,
                "table_id": table_id,
                "mode": materialization_mode,
                "materialize_to_datario": materialize_to_datario,
            },
            labels=current_flow_labels,
            run_name=f"Materialize {dataset_id}.{table_id}",
        )
        materialization_flow.set_upstream(CREATE_TABLE_AND_UPLOAD_TO_GCS_TASK)

        wait_for_materialization = wait_for_flow_run(
            materialization_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )
        wait_for_materialization.max_retries = (
            dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_ATTEMPTS.value
        )
        wait_for_materialization.retry_delay = timedelta(
            seconds=dump_db_constants.WAIT_FOR_MATERIALIZATION_RETRY_INTERVAL.value
        )

        with case(dump_to_gcs, True):
            # Trigger Dump to GCS flow run with project id as datario
            dump_to_gcs_flow = create_flow_run(
                flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
                project_name=constants.PREFECT_DEFAULT_PROJECT.value,
                parameters={
                    "project_id": "datario",
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "maximum_bytes_processed": maximum_bytes_processed,
                },
                labels=[
                    "datario",
                ],
                run_name=f"Dump to GCS {dataset_id}.{table_id}",
            )
            dump_to_gcs_flow.set_upstream(wait_for_materialization)

            wait_for_dump_to_gcs = wait_for_flow_run(
                dump_to_gcs_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=True,
            )

dump_url_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_url_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
