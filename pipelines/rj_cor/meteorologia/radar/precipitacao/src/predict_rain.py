# -*- coding: utf-8 -*-
# Attention: This file is licensed through the EULA license present in the license file at the root
# of this repository. Copying any part of this code is prohibited.
# flake8: noqa: E501
# pylint: skip-file
import datetime
import json
import pathlib

import h5py
import joblib
import numpy as np
import pandas as pd

from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.PredictedData import (
    PredictedData,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.integrity.check_integrity_ppi_hdf import (
    main as check_integrity,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.PointsToFeatures import (
    PointsToFeatures,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.process_ppi import (
    process_ppi,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.RadarData import (
    RadarData,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.data_utils import (
    NRAYS,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.featurization_utils import (
    parse_operation,
)
from pipelines.utils.utils import log


def run_model_prediction(
    base_path: str, verbose: bool = True, n_jobs: int = 1, output_hdf: bool = False
):
    """
    Run model predictions and save results
    """
    REF_LATLON = (-22.9932804107666, -43.58795928955078)

    prediction_specs_filepath = pathlib.Path(f"{base_path}/src/predict_rain_specs.json")

    with open(prediction_specs_filepath, "r") as json_file:
        prediction_specs_dict = json.load(json_file)
    log(f"[DEBUG] read json ok: {prediction_specs_dict}")
    input_hdfs = prediction_specs_dict["radar_ppi_hdfs"]
    coordinates_filepath = prediction_specs_dict["coordinates"]
    radar_cal_specs_dicts_filepath = prediction_specs_dict["radar_cal_specs"]
    indices_folder = prediction_specs_dict["indices_folder"]
    model_filepath = prediction_specs_dict["model_filepath"]
    output_path = prediction_specs_dict["output_path"]

    radar_cal_specs_dicts_filepath = pathlib.Path(radar_cal_specs_dicts_filepath)
    model_filepath = pathlib.Path(model_filepath)

    coordinates = []
    id_hex = []
    bairro = []

    with open(coordinates_filepath, "r") as f:
        for line in f:
            latlon = (float(line.split(",")[0]), float(line.split(",")[1]))
            coordinates.append(latlon)
            id_hex.append(line.split(",")[2])
            bairro.append(line.split(",")[3])
    log("[DEBUG] read coordinates filepath ok")
    with open(radar_cal_specs_dicts_filepath, "r") as json_file:
        json_list = list(json_file)
    dicts_list = []
    log("[DEBUG] read radar cal specs ok")
    for json_str in json_list:
        result = json.loads(json_str)
        dicts_list.append(result)

    radar_data_hdfs = []

    last_hdf = h5py.File(input_hdfs[-1])
    log("[DEBUG] read last hdf ok")
    hdf_date = last_hdf["what"].attrs["date"].decode("UTF-8")
    hdf_time = last_hdf["what"].attrs["time"].decode("UTF-8")

    dt = datetime.datetime.strptime(f"{hdf_date} {hdf_time[:-2]}", "%Y%m%d %H%M")
    prediction_dt = dt + datetime.timedelta(minutes=5)
    prediction_dt_str = prediction_dt.strftime("%Y%m%d %H%M%S")

    prediction_date, prediction_time = prediction_dt_str.split(" ")

    assert len(input_hdfs), "Number of passed radar hdf files must be 3."

    for hdf in input_hdfs:
        check_integrity(hdf)
        radar_data_dict = {}
        for specs_dict in dicts_list:
            if "_" in specs_dict["process_type"]:
                process, by_feature = specs_dict["process_type"].split("_")
                origin_radar_data = f"{process}-{by_feature}"
                index_matrix = radar_data_dict[origin_radar_data].indices
                new_radar_data = process_ppi(
                    verbose,
                    specs_dict["feature"],
                    specs_dict["process_type"],
                    hdf,
                    index_matrix,
                )
            else:
                new_radar_data = process_ppi(
                    verbose, specs_dict["feature"], specs_dict["process_type"], hdf
                )

            assert new_radar_data.nrays == NRAYS, f"nrays should be {NRAYS}."
            radar_data_dict[
                f"{specs_dict['process_type']}-{specs_dict['feature']}"
            ] = new_radar_data

        radar_data_hdfs.append(radar_data_dict)

    log("Radar data processed successfully.")

    coordinates = np.array(coordinates)

    # Get samplings to calculate indices

    sampling_set = set()
    for specs_dict in dicts_list:
        sampling_set = sampling_set.union(set(specs_dict["sampling"]))

    # Load pre-existing information (if it exists)

    indices_paths = pathlib.Path(indices_folder).rglob("*.npy")

    if len(list(indices_paths)):
        p2f = PointsToFeatures.load_indices(indices_folder)
        assert np.all(
            np.isclose(p2f.coordinates, coordinates)
        ), "Given coordinates are not the same as loaded ones."

        log("Indices loaded successfully.")
    else:
        p2f = PointsToFeatures(coordinates)
        log(
            "No indices saved: calculating...",
        )

    samplings_to_remove = set(p2f.indices)

    # Calculate remaining information

    sampling_set = sampling_set.difference(samplings_to_remove)

    for sampling in sampling_set:
        if "_nearest" in sampling:
            k, _ = sampling.split("_")
            k = int(k)
            p2f.calc_k_nearest(k, n_jobs)
        elif sampling == "beam":
            p2f.calc_beam(n_jobs)
        else:
            raise Exception(f"{sampling} sampling not implemented.")

    if len(sampling_set):
        p2f.save_indices(indices_folder)

        log("Indices saved successfully.")

    X = [p2f.get_dist()]

    for specs_dict in dicts_list:
        for rd_dict in radar_data_hdfs:
            process_key = specs_dict["process_type"] + "-" + specs_dict["feature"]
            rd = rd_dict[process_key]
            for operation in specs_dict["operations"]:
                feature = parse_operation(p2f, rd.unwrap_data(), operation)
                X.append(feature)

    X = np.hstack(X)
    X = np.where(X != np.inf, X, 0)

    # Make predictions

    log(f"Check joblib version {joblib.__version}")
    try:
        pipe = joblib.load(open(model_filepath, "rb"))
    except:
        pipe = joblib.load(model_filepath)
    y_pred = pipe.predict(X)

    log("Predictions made successfully.")

    output_path = pathlib.Path(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    model_parent_folders = str(model_filepath.parents[0]).split("/")[-2:]
    model = "/".join(model_parent_folders)

    predictions = PredictedData(
        y_pred,
        coordinates,
        radar_cal_specs_dicts_filepath.stem,
        model,
        prediction_date,
        prediction_time,
    )

    if output_hdf:
        predictions.save_hdf(output_path / "predictions.hdf")
    else:
        # np.savetxt(output_filepath, predictions.predictions, delimiter=",")
        log(f"[DEBUG] predictions.predictions: {predictions.predictions}")
        log(f"[DEBUG] predictions.coordinates: {predictions.coordinates}")
        log(f"[DEBUG] id_hex: {id_hex[:10]}")
        dfr = pd.DataFrame(
            predictions.coordinates, columns=["latitude", "longitude"]
        ).astype(float)
        dfr["predictions"] = predictions.predictions
        dfr["id_h3"] = id_hex
        dfr["bairro"] = bairro
        dfr["bairro"] = dfr["bairro"].str.replace("\n", "")
        dfr["data_medicao"] = pd.to_datetime(
            prediction_dt_str, format="%Y%m%d %H%M%S", utc=True
        )
        dfr["data_medicao"] = (
            dfr["data_medicao"]
            .dt.tz_convert("America/Sao_Paulo")
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )
        data_medicao = dfr["data_medicao"].max()
        log(f"DEBUUUUUUGGG data_medicao: {data_medicao}")
        log(f"DEBUUUUUUGGG {dfr.head()}")
        # dfr.to_csv(output_path / f"predictions_{data_medicao}.csv", index=False)
        return dfr
