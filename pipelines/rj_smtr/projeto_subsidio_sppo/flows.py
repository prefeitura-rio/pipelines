# -*- coding: utf-8 -*-
"""
Flows for projeto_subsidio_sppo
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefect.utilities.edges import unmapped

# EMD Imports #

from pipelines.constants import constants
from pipelines.utils.tasks import (
    rename_current_flow_run_now_time,
    get_now_date,
    get_yesterday,
    get_previous_date,
    get_current_flow_mode,
    get_current_flow_labels,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.tasks import get_k8s_dbt_client

# SMTR Imports #

from pipelines.rj_smtr.constants import constants as smtr_constants

from pipelines.rj_smtr.tasks import (
    fetch_dataset_sha,
    get_run_dates,
    get_join_dict,
    # get_local_dbt_client,
    # set_last_run_timestamp,
)

# from pipelines.rj_smtr.materialize_to_datario.flows import (
#     smtr_materialize_to_datario_viagem_sppo_flow,
# )

from pipelines.rj_smtr.veiculo.flows import (
    sppo_veiculo_dia,
)

from pipelines.rj_smtr.schedules import (
    every_day_hour_five,
    # every_dayofmonth_one_and_sixteen,
)
from pipelines.utils.execute_dbt_model.tasks import run_dbt_model

# Flows #

with Flow(
    "SMTR: Viagens SPPO",
    code_owners=["rodrigo", "fernanda"],
) as viagens_sppo:

    # Rename flow run
    current_date = get_now_date()

    # Get default parameters #
    date_range_start = Parameter("date_range_start", default=False)
    date_range_end = Parameter("date_range_end", default=False)

    run_dates = get_run_dates(date_range_start, date_range_end)

    rename_flow_run = rename_current_flow_run_now_time(
        prefix="SMTR - Viagens SPPO: ", now_time=run_dates
    )

    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    # Set dbt client #
    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value,
    )

    _vars = get_join_dict(dict_list=run_dates, new_dict=dataset_sha)

    RUN = run_dbt_model.map(
        dbt_client=unmapped(dbt_client),
        dataset_id=unmapped(smtr_constants.SUBSIDIO_SPPO_DATASET_ID.value),
        table_id=unmapped(smtr_constants.SUBSIDIO_SPPO_TABLE_ID.value),
        upstream=unmapped(True),
        exclude=unmapped("+gps_sppo"),
        _vars=_vars,
    )

viagens_sppo.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
viagens_sppo.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_SMTR_AGENT_LABEL.value]
)

viagens_sppo.schedule = every_day_hour_five

subsidio_sppo_apuracao_name = "SMTR: Subsídio SPPO Apuração"
with Flow(
    subsidio_sppo_apuracao_name,
    code_owners=["rodrigo"],
) as subsidio_sppo_apuracao:

    # 1. SETUP #

    # Get default parameters #
    start_date = Parameter("start_date", default=get_yesterday.run())
    end_date = Parameter("end_date", default=get_yesterday.run())
    stu_data_versao = Parameter("stu_data_versao", default=get_previous_date.run(6))
    materialize_sppo_veiculo_dia = Parameter("materialize_sppo_veiculo_dia", True)

    run_dates = get_run_dates(start_date, end_date)

    # Rename flow run #
    rename_flow_run = rename_current_flow_run_now_time(
        prefix=subsidio_sppo_apuracao_name + ": ", now_time=run_dates
    )

    # Set dbt client #
    LABELS = get_current_flow_labels()
    MODE = get_current_flow_mode(LABELS)

    dbt_client = get_k8s_dbt_client(mode=MODE, wait=rename_flow_run)
    # Use the command below to get the dbt client in dev mode:
    # dbt_client = get_local_dbt_client(host="localhost", port=3001)

    # Get models version #
    dataset_sha = fetch_dataset_sha(
        dataset_id=smtr_constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value,
    )

    # 2. MATERIALIZE DATA #
    with case(materialize_sppo_veiculo_dia, True):
        parameters = dict(
            start_date=start_date, end_date=end_date, stu_data_versao=stu_data_versao
        )

        SPPO_VEICULO_DIA_RUN = create_flow_run(
            flow_name=sppo_veiculo_dia.name,
            project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            run_name=sppo_veiculo_dia.name,
            parameters=parameters,
        )

        wait_for_flow_run(
            SPPO_VEICULO_DIA_RUN,
            stream_states=True,
            stream_logs=True,
            raise_final_state=True,
        )

        # 3. CALCULATE #
        SUBSIDIO_SPPO_APURACAO_RUN = run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=smtr_constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value,
            _vars=dict(start_date=start_date, end_date=end_date),
        )

        SPPO_VEICULO_DIA_RUN.set_downstream(SUBSIDIO_SPPO_APURACAO_RUN)

    with case(materialize_sppo_veiculo_dia, False):
        # 3. CALCULATE #
        run_dbt_model(
            dbt_client=dbt_client,
            dataset_id=smtr_constants.SUBSIDIO_SPPO_DASHBOARD_DATASET_ID.value,
            _vars=dict(start_date=start_date, end_date=end_date),
        )

    # # 3. PUBLISH #
    # run_materialize = create_flow_run(
    #     flow_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
    #     project_name=constants.PREFECT_DEFAULT_PROJECT.value,
    #     labels=[
    #         constants.RJ_DATARIO_AGENT_LABEL.value,
    #     ],
    #     run_name=smtr_materialize_to_datario_viagem_sppo_flow.name,
    #     parameters={
    #         "dataset_id": "transporte_rodoviario_municipal",
    #         "table_id": "viagem_onibus",
    #         "mode": "prod",
    #         "dbt_model_parameters": dict(end_date=end_date),
    #     },
    # )

subsidio_sppo_apuracao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
subsidio_sppo_apuracao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value, labels=[constants.RJ_SMTR_AGENT_LABEL.value]
)

# subsidio_sppo_apuracao.schedule = every_dayofmonth_one_and_sixteen
