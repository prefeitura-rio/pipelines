# -*- coding: utf-8 -*-
"""
Flows for registros_ocr_rir
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants as emd_constants

from pipelines.rj_smtr.constants import constants
from pipelines.rj_smtr.registros_ocr_rir.tasks import (
    get_files_from_ftp,
    download_and_save_local,
    pre_treatment_ocr,
)
from pipelines.rj_smtr.tasks import bq_upload, get_current_timestamp

from pipelines.rj_smtr.schedules import every_minute
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import rename_current_flow_run_now_time

with Flow(
    "SMTR - Captura FTP - OCR RIR",
    code_owners=[
        "caio",
        "fernanda",
    ],
) as captura_ocr:
    # SETUP
    dump = Parameter("dump", default=False)
    execution_time = Parameter("execution_time", default=None)
    rename_flow = rename_current_flow_run_now_time(
        prefix="OCR RIR: ", now_time=get_current_timestamp()
    )
    # Pipeline
    status = get_files_from_ftp(
        dump=dump, execution_time=execution_time, wait=rename_flow
    )
    with case(status["capture"], True):
        files = download_and_save_local(
            file_info=status["file_info"],
        )
        updated_status = pre_treatment_ocr(file_info=files)
        with case(updated_status["skip_upload"], False):
            bq_upload(
                dataset_id=constants.RIR_DATASET_ID.value,
                table_id=constants.RIR_TABLE_ID.value,
                filepath=updated_status["table_dir"],
                status={"error": None},
            )

captura_ocr.storage = GCS(emd_constants.GCS_FLOWS_BUCKET.value)
captura_ocr.run_config = KubernetesRun(
    image=emd_constants.DOCKER_IMAGE.value,
    labels=[emd_constants.RJ_SMTR_AGENT_LABEL.value],
)
# captura_ocr.schedule = every_minute
