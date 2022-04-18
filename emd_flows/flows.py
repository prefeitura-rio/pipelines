"""
Testing scheduled workflows
"""

from datetime import datetime

from prefect import Flow
from prefect.environments import KubernetesJobEnvironment
from prefect.run_configs import KubernetesRun
from prefect.storage import Module

from emd_flows.constants import constants
from emd_flows.schedules import (
    minute_schedule,
    five_minute_schedule,
    fifteen_minute_schedule,
)
from emd_flows.tasks import (
    get_random_api,
    fetch_from_api,
    csv_to_dataframe,
    preproc,
    log_to_discord,
)


with Flow("flow_0") as flow_0:
    ts = datetime.now()
    api_url = get_random_api()
    txt = fetch_from_api(api_url)
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_0")

flow_0.storage = Module("emd_flows.flows")
flow_0.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow_0.schedule = fifteen_minute_schedule
flow_0.environment = KubernetesJobEnvironment(
    labels=[constants.K8S_AGENT_LABEL.value]
)


with Flow("flow_1") as flow_1:
    ts = datetime.now()
    api_url = get_random_api()
    txt = fetch_from_api(api_url)
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_1")

flow_1.storage = Module("emd_flows.flows")
flow_1.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow_1.schedule = fifteen_minute_schedule


with Flow("flow_2") as flow_2:
    ts = datetime.now()
    api_url = get_random_api()
    txt = fetch_from_api(api_url)
    df = csv_to_dataframe(txt)
    df = preproc(dataframe=df)
    log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_2")

flow_2.storage = Module("emd_flows.flows")
flow_2.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
flow_2.schedule = five_minute_schedule


# with Flow("flow_3") as flow_3:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_3")

# flow_3.storage = Module("emd_flows.flows")
# flow_3.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_3.schedule = fifteen_minute_schedule


# with Flow("flow_4") as flow_4:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_4")

# flow_4.storage = Module("emd_flows.flows")
# flow_4.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_4.schedule = minute_schedule


# with Flow("flow_5") as flow_5:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_5")

# flow_5.storage = Module("emd_flows.flows")
# flow_5.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_5.schedule = minute_schedule


# with Flow("flow_6") as flow_6:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_6")

# flow_6.storage = Module("emd_flows.flows")
# flow_6.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_6.schedule = minute_schedule


# with Flow("flow_7") as flow_7:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_7")

# flow_7.storage = Module("emd_flows.flows")
# flow_7.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_7.schedule = minute_schedule


# with Flow("flow_8") as flow_8:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_8")

# flow_8.storage = Module("emd_flows.flows")
# flow_8.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_8.schedule = minute_schedule


# with Flow("flow_9") as flow_9:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_9")

# flow_9.storage = Module("emd_flows.flows")
# flow_9.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_9.schedule = fifteen_minute_schedule


# with Flow("flow_10") as flow_10:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_10")

# flow_10.storage = Module("emd_flows.flows")
# flow_10.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_10.schedule = minute_schedule


# with Flow("flow_11") as flow_11:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_11")

# flow_11.storage = Module("emd_flows.flows")
# flow_11.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_11.schedule = fifteen_minute_schedule


# with Flow("flow_12") as flow_12:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_12")

# flow_12.storage = Module("emd_flows.flows")
# flow_12.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_12.schedule = minute_schedule


# with Flow("flow_13") as flow_13:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_13")

# flow_13.storage = Module("emd_flows.flows")
# flow_13.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_13.schedule = minute_schedule


# with Flow("flow_14") as flow_14:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_14")

# flow_14.storage = Module("emd_flows.flows")
# flow_14.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_14.schedule = five_minute_schedule


# with Flow("flow_15") as flow_15:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_15")

# flow_15.storage = Module("emd_flows.flows")
# flow_15.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_15.schedule = minute_schedule


# with Flow("flow_16") as flow_16:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_16")

# flow_16.storage = Module("emd_flows.flows")
# flow_16.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_16.schedule = five_minute_schedule


# with Flow("flow_17") as flow_17:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_17")

# flow_17.storage = Module("emd_flows.flows")
# flow_17.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_17.schedule = five_minute_schedule


# with Flow("flow_18") as flow_18:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_18")

# flow_18.storage = Module("emd_flows.flows")
# flow_18.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_18.schedule = fifteen_minute_schedule


# with Flow("flow_19") as flow_19:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_19")

# flow_19.storage = Module("emd_flows.flows")
# flow_19.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_19.schedule = five_minute_schedule


# with Flow("flow_20") as flow_20:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_20")

# flow_20.storage = Module("emd_flows.flows")
# flow_20.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_20.schedule = fifteen_minute_schedule


# with Flow("flow_21") as flow_21:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_21")

# flow_21.storage = Module("emd_flows.flows")
# flow_21.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_21.schedule = minute_schedule


# with Flow("flow_22") as flow_22:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_22")

# flow_22.storage = Module("emd_flows.flows")
# flow_22.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_22.schedule = five_minute_schedule


# with Flow("flow_23") as flow_23:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_23")

# flow_23.storage = Module("emd_flows.flows")
# flow_23.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_23.schedule = fifteen_minute_schedule


# with Flow("flow_24") as flow_24:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_24")

# flow_24.storage = Module("emd_flows.flows")
# flow_24.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_24.schedule = minute_schedule


# with Flow("flow_25") as flow_25:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_25")

# flow_25.storage = Module("emd_flows.flows")
# flow_25.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_25.schedule = five_minute_schedule


# with Flow("flow_26") as flow_26:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_26")

# flow_26.storage = Module("emd_flows.flows")
# flow_26.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_26.schedule = minute_schedule


# with Flow("flow_27") as flow_27:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_27")

# flow_27.storage = Module("emd_flows.flows")
# flow_27.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_27.schedule = minute_schedule


# with Flow("flow_28") as flow_28:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_28")

# flow_28.storage = Module("emd_flows.flows")
# flow_28.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_28.schedule = minute_schedule


# with Flow("flow_29") as flow_29:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_29")

# flow_29.storage = Module("emd_flows.flows")
# flow_29.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_29.schedule = five_minute_schedule


# with Flow("flow_30") as flow_30:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_30")

# flow_30.storage = Module("emd_flows.flows")
# flow_30.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_30.schedule = minute_schedule


# with Flow("flow_31") as flow_31:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_31")

# flow_31.storage = Module("emd_flows.flows")
# flow_31.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_31.schedule = minute_schedule


# with Flow("flow_32") as flow_32:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_32")

# flow_32.storage = Module("emd_flows.flows")
# flow_32.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_32.schedule = fifteen_minute_schedule


# with Flow("flow_33") as flow_33:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_33")

# flow_33.storage = Module("emd_flows.flows")
# flow_33.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_33.schedule = minute_schedule


# with Flow("flow_34") as flow_34:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_34")

# flow_34.storage = Module("emd_flows.flows")
# flow_34.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_34.schedule = five_minute_schedule


# with Flow("flow_35") as flow_35:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_35")

# flow_35.storage = Module("emd_flows.flows")
# flow_35.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_35.schedule = five_minute_schedule


# with Flow("flow_36") as flow_36:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_36")

# flow_36.storage = Module("emd_flows.flows")
# flow_36.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_36.schedule = minute_schedule


# with Flow("flow_37") as flow_37:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_37")

# flow_37.storage = Module("emd_flows.flows")
# flow_37.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_37.schedule = minute_schedule


# with Flow("flow_38") as flow_38:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_38")

# flow_38.storage = Module("emd_flows.flows")
# flow_38.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_38.schedule = fifteen_minute_schedule


# with Flow("flow_39") as flow_39:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_39")

# flow_39.storage = Module("emd_flows.flows")
# flow_39.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_39.schedule = five_minute_schedule


# with Flow("flow_40") as flow_40:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_40")

# flow_40.storage = Module("emd_flows.flows")
# flow_40.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_40.schedule = five_minute_schedule


# with Flow("flow_41") as flow_41:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_41")

# flow_41.storage = Module("emd_flows.flows")
# flow_41.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_41.schedule = fifteen_minute_schedule


# with Flow("flow_42") as flow_42:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_42")

# flow_42.storage = Module("emd_flows.flows")
# flow_42.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_42.schedule = fifteen_minute_schedule


# with Flow("flow_43") as flow_43:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_43")

# flow_43.storage = Module("emd_flows.flows")
# flow_43.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_43.schedule = fifteen_minute_schedule


# with Flow("flow_44") as flow_44:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_44")

# flow_44.storage = Module("emd_flows.flows")
# flow_44.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_44.schedule = fifteen_minute_schedule


# with Flow("flow_45") as flow_45:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_45")

# flow_45.storage = Module("emd_flows.flows")
# flow_45.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_45.schedule = five_minute_schedule


# with Flow("flow_46") as flow_46:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_46")

# flow_46.storage = Module("emd_flows.flows")
# flow_46.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_46.schedule = five_minute_schedule


# with Flow("flow_47") as flow_47:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_47")

# flow_47.storage = Module("emd_flows.flows")
# flow_47.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_47.schedule = five_minute_schedule


# with Flow("flow_48") as flow_48:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_48")

# flow_48.storage = Module("emd_flows.flows")
# flow_48.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_48.schedule = five_minute_schedule


# with Flow("flow_49") as flow_49:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_49")

# flow_49.storage = Module("emd_flows.flows")
# flow_49.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_49.schedule = five_minute_schedule


# with Flow("flow_50") as flow_50:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_50")

# flow_50.storage = Module("emd_flows.flows")
# flow_50.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_50.schedule = minute_schedule


# with Flow("flow_51") as flow_51:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_51")

# flow_51.storage = Module("emd_flows.flows")
# flow_51.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_51.schedule = five_minute_schedule


# with Flow("flow_52") as flow_52:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_52")

# flow_52.storage = Module("emd_flows.flows")
# flow_52.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_52.schedule = five_minute_schedule


# with Flow("flow_53") as flow_53:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_53")

# flow_53.storage = Module("emd_flows.flows")
# flow_53.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_53.schedule = fifteen_minute_schedule


# with Flow("flow_54") as flow_54:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_54")

# flow_54.storage = Module("emd_flows.flows")
# flow_54.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_54.schedule = minute_schedule


# with Flow("flow_55") as flow_55:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_55")

# flow_55.storage = Module("emd_flows.flows")
# flow_55.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_55.schedule = five_minute_schedule


# with Flow("flow_56") as flow_56:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_56")

# flow_56.storage = Module("emd_flows.flows")
# flow_56.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_56.schedule = five_minute_schedule


# with Flow("flow_57") as flow_57:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_57")

# flow_57.storage = Module("emd_flows.flows")
# flow_57.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_57.schedule = fifteen_minute_schedule


# with Flow("flow_58") as flow_58:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_58")

# flow_58.storage = Module("emd_flows.flows")
# flow_58.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_58.schedule = fifteen_minute_schedule


# with Flow("flow_59") as flow_59:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_59")

# flow_59.storage = Module("emd_flows.flows")
# flow_59.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_59.schedule = five_minute_schedule


# with Flow("flow_60") as flow_60:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_60")

# flow_60.storage = Module("emd_flows.flows")
# flow_60.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_60.schedule = five_minute_schedule


# with Flow("flow_61") as flow_61:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_61")

# flow_61.storage = Module("emd_flows.flows")
# flow_61.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_61.schedule = five_minute_schedule


# with Flow("flow_62") as flow_62:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_62")

# flow_62.storage = Module("emd_flows.flows")
# flow_62.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_62.schedule = fifteen_minute_schedule


# with Flow("flow_63") as flow_63:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_63")

# flow_63.storage = Module("emd_flows.flows")
# flow_63.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_63.schedule = five_minute_schedule


# with Flow("flow_64") as flow_64:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_64")

# flow_64.storage = Module("emd_flows.flows")
# flow_64.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_64.schedule = minute_schedule


# with Flow("flow_65") as flow_65:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_65")

# flow_65.storage = Module("emd_flows.flows")
# flow_65.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_65.schedule = fifteen_minute_schedule


# with Flow("flow_66") as flow_66:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_66")

# flow_66.storage = Module("emd_flows.flows")
# flow_66.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_66.schedule = minute_schedule


# with Flow("flow_67") as flow_67:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_67")

# flow_67.storage = Module("emd_flows.flows")
# flow_67.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_67.schedule = minute_schedule


# with Flow("flow_68") as flow_68:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_68")

# flow_68.storage = Module("emd_flows.flows")
# flow_68.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_68.schedule = fifteen_minute_schedule


# with Flow("flow_69") as flow_69:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_69")

# flow_69.storage = Module("emd_flows.flows")
# flow_69.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_69.schedule = fifteen_minute_schedule


# with Flow("flow_70") as flow_70:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_70")

# flow_70.storage = Module("emd_flows.flows")
# flow_70.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_70.schedule = fifteen_minute_schedule


# with Flow("flow_71") as flow_71:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_71")

# flow_71.storage = Module("emd_flows.flows")
# flow_71.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_71.schedule = minute_schedule


# with Flow("flow_72") as flow_72:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_72")

# flow_72.storage = Module("emd_flows.flows")
# flow_72.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_72.schedule = fifteen_minute_schedule


# with Flow("flow_73") as flow_73:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_73")

# flow_73.storage = Module("emd_flows.flows")
# flow_73.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_73.schedule = fifteen_minute_schedule


# with Flow("flow_74") as flow_74:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_74")

# flow_74.storage = Module("emd_flows.flows")
# flow_74.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_74.schedule = five_minute_schedule


# with Flow("flow_75") as flow_75:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_75")

# flow_75.storage = Module("emd_flows.flows")
# flow_75.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_75.schedule = five_minute_schedule


# with Flow("flow_76") as flow_76:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_76")

# flow_76.storage = Module("emd_flows.flows")
# flow_76.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_76.schedule = five_minute_schedule


# with Flow("flow_77") as flow_77:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_77")

# flow_77.storage = Module("emd_flows.flows")
# flow_77.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_77.schedule = fifteen_minute_schedule


# with Flow("flow_78") as flow_78:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_78")

# flow_78.storage = Module("emd_flows.flows")
# flow_78.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_78.schedule = five_minute_schedule


# with Flow("flow_79") as flow_79:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_79")

# flow_79.storage = Module("emd_flows.flows")
# flow_79.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_79.schedule = fifteen_minute_schedule


# with Flow("flow_80") as flow_80:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_80")

# flow_80.storage = Module("emd_flows.flows")
# flow_80.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_80.schedule = five_minute_schedule


# with Flow("flow_81") as flow_81:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_81")

# flow_81.storage = Module("emd_flows.flows")
# flow_81.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_81.schedule = fifteen_minute_schedule


# with Flow("flow_82") as flow_82:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_82")

# flow_82.storage = Module("emd_flows.flows")
# flow_82.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_82.schedule = five_minute_schedule


# with Flow("flow_83") as flow_83:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_83")

# flow_83.storage = Module("emd_flows.flows")
# flow_83.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_83.schedule = fifteen_minute_schedule


# with Flow("flow_84") as flow_84:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_84")

# flow_84.storage = Module("emd_flows.flows")
# flow_84.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_84.schedule = five_minute_schedule


# with Flow("flow_85") as flow_85:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_85")

# flow_85.storage = Module("emd_flows.flows")
# flow_85.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_85.schedule = minute_schedule


# with Flow("flow_86") as flow_86:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_86")

# flow_86.storage = Module("emd_flows.flows")
# flow_86.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_86.schedule = fifteen_minute_schedule


# with Flow("flow_87") as flow_87:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_87")

# flow_87.storage = Module("emd_flows.flows")
# flow_87.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_87.schedule = five_minute_schedule


# with Flow("flow_88") as flow_88:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_88")

# flow_88.storage = Module("emd_flows.flows")
# flow_88.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_88.schedule = fifteen_minute_schedule


# with Flow("flow_89") as flow_89:
#     ts = datetime.now()
#     api_url = get_random_api()
#     txt = fetch_from_api(api_url)
#     df = csv_to_dataframe(txt)
#     df = preproc(dataframe=df)
#     log_to_discord(dataframe=df, timestamp=ts, wf_name="flow_89")

# flow_89.storage = Module("emd_flows.flows")
# flow_89.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# flow_89.schedule = five_minute_schedule
