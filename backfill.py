# -*- coding: utf-8 -*-
"""
Helper script for running backfill jobs.
"""

from copy import deepcopy
from pathlib import Path
from time import sleep, time
from typing import List

from loguru import logger
import pandas as pd
import pendulum
from prefect import Client

from pipelines.utils.utils import run_registered

OUTPUT_FILENAME = "backfill.csv"

FLOW_NAME = "INEA: Teste"
HELP_NAME = "Radar INEA"
AGENT_LABEL = "inea"
PARAMETER_DEFAULTS = {
    "bucket_name": "rj-escritorio-dev",
    "date": None,
    "mode": "prod",
    "prefix": "raw/meio_ambiente_clima/inea_radar",
    "product": "ppi",
    "radar": "gua",
}

BACKFILL_START = pendulum.datetime(year=2016, month=1, day=1)
BACKFILL_END = pendulum.datetime(year=2022, month=9, day=29)
BACKFILL_INTERVAL = pendulum.duration(days=1)
REVERSE = True
DATETIME_FORMAT = "YYYYMMDD"
DATETIME_START_PARAM = "date"
DATETIME_END_PARAM = None


def create_timestamp_parameters(
    start: pendulum.DateTime,
    end: pendulum.DateTime,
    interval: pendulum.Duration,
    format: str = "YYYY-MM-DD",
    reverse: bool = False,
) -> List[str]:
    """
    Create a list of parameters for a flow that takes timestamp parameters.
    """
    parameters = []
    while start < end:
        this_end = start + interval
        parameters.append(
            {
                "start": start.format(format),
                "end": this_end.format(format),
            }
        )
        start = this_end
    if reverse:
        parameters.reverse()
    return parameters


if __name__ == "__main__":

    timestamp_parameters = create_timestamp_parameters(
        start=BACKFILL_START,
        end=BACKFILL_END,
        interval=BACKFILL_INTERVAL,
        format=DATETIME_FORMAT,
        reverse=REVERSE,
    )

    client = Client()

    if Path(OUTPUT_FILENAME).exists():
        df: pd.DataFrame = pd.read_csv(OUTPUT_FILENAME)
    else:
        df: pd.DataFrame = pd.DataFrame(
            columns=[
                "flow_id",
                "flow_run_id",
                "run_start",
                "run_end",
                "run_state",
                "run_parameters",
            ]
        )

    start_time = time()
    runs_count = 1e-6

    for parameters in timestamp_parameters:
        this_run_parameters = deepcopy(PARAMETER_DEFAULTS)

        if DATETIME_START_PARAM:
            this_run_parameters[DATETIME_START_PARAM] = parameters["start"]

        if DATETIME_END_PARAM:
            this_run_parameters[DATETIME_END_PARAM] = parameters["end"]

        logger.info(
            f"Launching run for window {parameters['start']} to {parameters['end']}"
        )
        flow_run_id = run_registered(
            flow_name=FLOW_NAME,
            labels=[AGENT_LABEL],
            parameters=this_run_parameters,
            run_description=f"Backfill {HELP_NAME}",
        )

        state = client.get_flow_run_info(flow_run_id=flow_run_id).state

        while not state.is_finished():
            state = client.get_flow_run_info(flow_run_id=flow_run_id).state
            sleep(0.5)

        if state.is_successful():
            logger.success(f"✅ {parameters['start']} - {parameters['end']}")
        else:
            logger.error(f"❌ {parameters['start']} - {parameters['end']}")

        runs_count += 1
        logger.info(
            f"Average run time: {(time() - start_time) / runs_count:.2f} seconds"
        )

        df: pd.DataFrame = df.append(
            {
                "flow_name": FLOW_NAME,
                "flow_run_id": flow_run_id,
                "run_start": parameters["start"],
                "run_end": parameters["end"],
                "run_state": state,
                "run_parameters": this_run_parameters,
            },
            ignore_index=True,
        )

        df.to_csv(OUTPUT_FILENAME, index=False)
