# -*- coding: utf-8 -*-
import pandas as pd

from pipelines.rj_smtr.br_rj_riodejaneiro_brt_prediction.constants import constants

gtfs_brt_treated_names = [
    "agency",
    "calendar_dates",
    "calendar",
    "frequencies",
    "routes",
    "shapes",
    "stop_times",
    "stops",
    "trips",
]

for name in gtfs_brt_treated_names:
    print(f"Uploading {name}")
    pd.read_csv(
        f"{constants.CURRENT_DIR.value}/Dados/gtfs_brt_treated/{name}.csv"
    ).to_csv(
        f"{constants.BRT_PREDICTOR_BUCKET.value}/dados/gtfs-brt-treated/{name}.csv",
        index=False,
    )
