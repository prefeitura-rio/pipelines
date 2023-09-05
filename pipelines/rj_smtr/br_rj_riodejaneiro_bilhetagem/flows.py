# -*- coding: utf-8 -*-
"""
Flows for br_rj_riodejaneiro_bilhetagem
"""

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefect import case, Parameter
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

# EMD Imports #

from pipelines.constants import constants as emd_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_current_flow_labels,
    get_current_flow_mode,
)

# SMTR Imports #

from pipelines.rj_smtr.constants import constants

from pipelines.rj_smtr.tasks import (
    create_date_partition,
    create_date_hour_partition,
    create_local_partition_path,
    get_current_timestamp,
    get_raw,
    parse_timestamp_to_string,
    save_raw_local,
    save_treated_local,
    upload_logs_to_bq,
    bq_upload,
    pre_treatment_nest_data,
    get_recapture_timestamps,
)

from pipelines.rj_smtr.utils import get_project_name

from pipelines.rj_smtr.schedules import every_10_minutes

from pipelines.rj_smtr.br_rj_riodejaneiro_bilhetagem.tasks import (
    get_bilhetagem_params,
    get_bilhetagem_url,
    get_datetime_range,
)

# Flows #

BILHETAGEM_TRANSACAO_RECAPTURA_FLOW_NAME = "SMTR: Bilhetagem Transação (recaptura)"

with Flow(
    BILHETAGEM_TRANSACAO_RECAPTURA_FLOW_NAME,
    code_owners=["caio", "fernanda", "boris", "rodrigo"],
) as bilhetagem_transacao_recaptura:
    start_date = Parameter("start_date", default="")
    end_date = Parameter("end_date", default="")

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)
    PROJECT_NAME = get_project_name(MODE)

    current_timestamp = get_current_timestamp()

    timestamps = get_recapture_timestamps(
        current_timestamp=current_timestamp,
        start_date=start_date,
        end_date=end_date,
        dataset_id=constants.BILHETAGEM_DATASET_ID.value,
        table_id=constants.BILHETAGEM_TRANSACAO_TABLE_ID.value,
    )

    BILHETAGEM_TRANSACAO_RECAPTURA_RUN = create_flow_run.map(
        flow_name=unmapped("SMTR: Bilhetagem Transação (captura)"),
        project_name=unmapped(PROJECT_NAME),
        run_name=unmapped("SMTR: Bilhetagem Transação (captura)"),
        parameters=timestamps,
    )

    wait_for_flow_run.map(
        BILHETAGEM_TRANSACAO_RECAPTURA_RUN,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

bilhetagem_transacao_recaptura.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
bilhetagem_transacao_recaptura.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
bilhetagem_transacao_recaptura.schedule = every_10_minutes
