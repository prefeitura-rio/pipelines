# -*- coding: utf-8 -*-
"""
Flow definition.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

from pipelines.constants import constants
from pipelines.rj_escritorio.notify_flooding.schedules import test_schedule
from pipelines.rj_escritorio.notify_flooding.tasks import (
    parse_comma_separated_string_to_list,
    get_open_occurrences,
    filter_flooding_occurences,
    get_cached_flooding_occurences,
    compare_flooding_occurences,
    update_flooding_occurences_cache,
    send_email_for_flooding_occurence,
)
from pipelines.utils.decorators import Flow

with Flow(
    name="EMD: Waze - Notificar ocorrências de alagamento",
    code_owners=[
        "gabriel",
    ],
) as rj_escritorio_notify_flooding_flow:

    # Parameters
    api_url = Parameter(
        "api_url",
        default="https://api.dados.rio/v2/adm_cor_comando/ocorrencias_abertas/",
    )
    email_configuration_secret_path = Parameter("email_configuration_secret_path")
    flooding_pop_id = Parameter(
        "flooding_pop_id",
        default="6,31",
    )
    redis_key = Parameter(
        "redis_key",
        default="rj_escritorio_notify_flooding_flow_cached_flooding_occurences",
    )
    to_email = Parameter("to_email")

    # Flow
    all_open_occurences = get_open_occurrences(api_url=api_url)
    flooding_pop_id = parse_comma_separated_string_to_list(
        input_text=flooding_pop_id, output_type=int
    )
    to_email = parse_comma_separated_string_to_list(
        input_text=to_email, output_type=str
    )
    open_flooding_occurences = filter_flooding_occurences(
        open_occurrences=all_open_occurences, flooding_pop_id=flooding_pop_id
    )
    cached_flooding_occurences = get_cached_flooding_occurences(redis_key=redis_key)
    (
        new_flooding_occurences,
        closed_flooding_occurences,
        current_flooding_occurences,
    ) = compare_flooding_occurences(
        from_api=open_flooding_occurences,
        from_cache=cached_flooding_occurences,
    )
    update_flooding_occurences_cache(
        flooding_occurrences=current_flooding_occurences, redis_key=redis_key
    )
    send_email_for_flooding_occurence.map(
        occurence=new_flooding_occurences,
        mode=unmapped("new"),
        to_email=unmapped(to_email),
        email_configuration_secret_path=unmapped(email_configuration_secret_path),
    )
    send_email_for_flooding_occurence.map(
        occurence=closed_flooding_occurences,
        mode=unmapped("closed"),
        to_email=unmapped(to_email),
        email_configuration_secret_path=unmapped(email_configuration_secret_path),
    )

rj_escritorio_notify_flooding_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_notify_flooding_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_ESCRITORIO_DEV_AGENT_LABEL.value],
)
rj_escritorio_notify_flooding_flow.schedule = test_schedule
