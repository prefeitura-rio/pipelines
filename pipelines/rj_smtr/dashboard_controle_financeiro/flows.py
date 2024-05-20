# -*- coding: utf-8 -*-
# pylint: disable=W0511
"""
Flows for veiculos
"""


from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.rj_smtr.dashboard_controle_financeiro.tasks import (
    pre_treatment_controle_cct,
)
from pipelines.utils.decorators import Flow


# SMTR Imports #

from pipelines.rj_smtr.constants import constants


from pipelines.rj_smtr.tasks import (
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_raw,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
)


# Flows #

# flake8: noqa: E501
with Flow(
    f"SMTR: {constants.CSV_CONTROLE_CCT_CB.value['dataset_id']} {constants.CSV_CONTROLE_CCT_CB.value['table_id']} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as controle_cct_cb_captura:

    # SETUP #
    timestamp = get_current_timestamp()
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.CSV_CONTROLE_CCT_CB.value["dataset_id"],
        table_id=constants.CSV_CONTROLE_CCT_CB.value["table_id"],
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT #
    raw_status = get_raw(
        url=constants.CSV_CONTROLE_CCT_CB.value["url"],
        filetype="csv",
    )

    raw_file_paths = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_controle_cct(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.CSV_CONTROLE_CCT_CB.value["dataset_id"],
        table_id=constants.CSV_CONTROLE_CCT_CB.value["table_id"],
        filepath=treated_filepath,
        raw_filepath=raw_file_paths,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.CSV_CONTROLE_CCT_CB.value["dataset_id"],
        parent_table_id=constants.CSV_CONTROLE_CCT_CB.value["table_id"],
        timestamp=timestamp,
        error=error,
    )

controle_cct_cb_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cb_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
# controle_cct_cb_captura.schedule = every_day_hour_seven

with Flow(
    f"SMTR: {constants.CSV_CONTROLE_CCT_CETT.value['dataset_id']} {constants.CSV_CONTROLE_CCT_CETT.value['table_id']} - Captura",
    # code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as controle_cct_cett_captura:

    # SETUP #
    timestamp = get_current_timestamp()
    partitions = create_date_hour_partition(timestamp, partition_date_only=True)

    filename = parse_timestamp_to_string(timestamp)

    filepath = create_local_partition_path(
        dataset_id=constants.CSV_CONTROLE_CCT_CETT.value["dataset_id"],
        table_id=constants.CSV_CONTROLE_CCT_CETT.value["table_id"],
        filename=filename,
        partitions=partitions,
    )

    # EXTRACT #
    raw_status = get_raw(
        url=constants.CSV_CONTROLE_CCT_CETT.value["url"],
        filetype="csv",
    )

    raw_file_paths = save_raw_local(status=raw_status, file_path=filepath)

    # TREAT
    treated_status = pre_treatment_controle_cct(status=raw_status, timestamp=timestamp)

    treated_filepath = save_treated_local(status=treated_status, file_path=filepath)

    # LOAD
    error = bq_upload(
        dataset_id=constants.CSV_CONTROLE_CCT_CETT.value["dataset_id"],
        table_id=constants.CSV_CONTROLE_CCT_CETT.value["table_id"],
        filepath=treated_filepath,
        raw_filepath=raw_file_paths,
        partitions=partitions,
        status=treated_status,
    )
    upload_logs_to_bq(
        dataset_id=constants.CSV_CONTROLE_CCT_CETT.value["dataset_id"],
        parent_table_id=constants.CSV_CONTROLE_CCT_CETT.value["table_id"],
        timestamp=timestamp,
        error=error,
    )

controle_cct_cett_captura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
controle_cct_cett_captura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_DEV_AGENT_LABEL.value],
)
# controle_cct_cett_captura.schedule = every_day_hour_seven
