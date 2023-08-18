# -*- coding: utf-8 -*-
from prefect import Parameter, Flow
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
)


with Flow(
    name="SMS: Farmacia - Upload Organization", code_owners=["thiago", "andre"]
) as upload_organizations:

    #  GCP
    dataset_id = "dump_cnes"
    table_id = "tbEstabelecimento"
    dump_mode = "overwrite"  # append or overwrite

    upload_task = create_table_and_upload_to_gcs(
        data_path= "~\tmp\tbEstabelecimento202307_REV.parquet",
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=True,
        wait=None,
    )


upload_organizations.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
upload_organizations.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)