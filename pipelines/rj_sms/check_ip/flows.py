# -*- coding: utf-8 -*-
from pipelines.utils.decorators import Flow
from pipelines.constants import constants
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.rj_sms.check_ip.tasks import get_public_ip


with Flow(
    name="SMS: Check IP - Verifica ip do cluster", code_owners=["thiago"]
) as check_ip:

    # Start run
    download_task = get_public_ip()


check_ip.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
check_ip.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_DEV_AGENT_LABEL.value,
    ],
)
