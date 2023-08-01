# -*- coding: utf-8 -*-
"""
Exemplo SMS
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.rj_sms.exemplo_sms.schedules import every_5_minutes
from pipelines.rj_sms.exemplo_sms.tasks import say_hello
from pipelines.utils.decorators import Flow


with Flow(
    name="SMS: exemplo - Exemplo para SMS",
) as rj_sms_exemplo_sms_hello_flow:

    # Parameters
    name = Parameter("name", default="World")

    # Tasks
    say_hello(name)

rj_sms_exemplo_sms_hello_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_sms_exemplo_sms_hello_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
rj_sms_exemplo_sms_hello_flow.schedule = every_5_minutes
