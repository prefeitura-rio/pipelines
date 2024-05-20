# a task that download a csv file from a url and upload it to a BigQuery table
from datetime import datetime
import pandas as pd
from prefect import task


from pipelines.rj_smtr.constants import constants
from pipelines.utils.utils import log
from pipelines.rj_smtr.utils import data_info_str


@task
def pre_treatment_controle_cct(status: dict, timestamp: datetime) -> None:

    if status["error"] is not None:
        return {"data": pd.DataFrame(), "error": status["error"]}

    try:
        error = None
        data = pd.json_normalize(status["data"])

        log(
            f"""
        Received inputs:
        - timestamp:\n{timestamp}
        - data:\n{data.head()}"""
        )

        log(f"Raw data:\n{data_info_str(data)}", level="info")

        log("Renaming columns...", level="info")
        data.columns = [col.encode("latin1").decode("utf-8") for col in data.columns]
        data = data.rename(columns=constants.CSV_CONTROLE_CCT_COLUMNS.value)

        data = data.applymap(
            lambda x: (
                str(x).encode("latin1").decode("utf-8") if isinstance(x, object) else x
            )
        )

        log("Adding captured timestamp column...", level="info")
        data["timestamp_captura"] = timestamp

        log("Striping string columns...", level="info")
        for col in data.columns[data.dtypes == "object"].to_list():
            data[col] = data[col].str.strip()

        log(
            f"Finished cleaning! Pre-treated data:\n{data_info_str(data)}", level="info"
        )
    except Exception as exp:  # pylint: disable=W0703
        error = exp

    if error is not None:
        log(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}
