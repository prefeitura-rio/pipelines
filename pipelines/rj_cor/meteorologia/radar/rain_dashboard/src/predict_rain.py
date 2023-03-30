# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: skip-file
import argparse
import datetime
import json
import pathlib

import h5py
import joblib
import numpy as np
import pandas as pd

from src.data.PredictedData import PredictedData
from src.data.process.integrity.check_integrity_ppi_hdf import main as check_integrity
from src.data.process.PointsToFeatures import PointsToFeatures
from src.data.process.process_ppi import process_ppi
from src.data.process.RadarData import RadarData
from src.utils.data_utils import NRAYS
from src.utils.featurization_utils import parse_operation
from src.utils.general_utils import print_ok, print_warning

REF_LATLON = (-22.9932804107666, -43.58795928955078)

parser = argparse.ArgumentParser()
parser.add_argument(
    "--verbose",
    "-v",
    help="If true, prints verbose output.",
    action="store_true",
)
parser.add_argument(
    "--specs_filepath",
    "-sf",
    help="Filepath of specifications for predictions.",
    type=str,
)
parser.add_argument(
    "--n_jobs", "-nj", help="Number of jobs to parallelize.", type=int, default=1
)
parser.add_argument(
    "--output_hdf",
    "-oh",
    help="If true, saves predictions in hdf format through PredictedData class. Else, saves as csv",
    action="store_true",
)
args = parser.parse_args()


prediction_specs_filepath = pathlib.Path(args.specs_filepath)

with open(prediction_specs_filepath, "r") as json_file:
    prediction_specs_dict = json.load(json_file)

input_hdfs = prediction_specs_dict["radar_ppi_hdfs"]
coordinates_filepath = prediction_specs_dict["coordinates"]
radar_cal_specs_dicts_filepath = prediction_specs_dict["radar_cal_specs"]
indices_folder = prediction_specs_dict["indices_folder"]
model_filepath = prediction_specs_dict["model_filepath"]
output_path = prediction_specs_dict["output_path"]

radar_cal_specs_dicts_filepath = pathlib.Path(radar_cal_specs_dicts_filepath)
model_filepath = pathlib.Path(model_filepath)

coordinates = []

with open(coordinates_filepath, "r") as f:
    for line in f:
        latlon = (float(line.split(",")[0]), float(line.split(",")[1]))
        coordinates.append(latlon)

with open(radar_cal_specs_dicts_filepath, "r") as json_file:
    json_list = list(json_file)
dicts_list = []

for json_str in json_list:
    result = json.loads(json_str)
    dicts_list.append(result)

radar_data_hdfs = []

last_hdf = h5py.File(input_hdfs[-1])

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
                args.verbose,
                specs_dict["feature"],
                specs_dict["process_type"],
                hdf,
                index_matrix,
            )
        else:
            new_radar_data = process_ppi(
                args.verbose, specs_dict["feature"], specs_dict["process_type"], hdf
            )

        assert new_radar_data.nrays == NRAYS, f"nrays should be {NRAYS}."
        radar_data_dict[
            f"{specs_dict['process_type']}-{specs_dict['feature']}"
        ] = new_radar_data

    radar_data_hdfs.append(radar_data_dict)

print_ok("Radar data processed successfully.", args.verbose)

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

    print_ok("Indices loaded successfully.", args.verbose)
else:
    p2f = PointsToFeatures(coordinates)
    print_warning("No indices saved: calculating...", args.verbose)

samplings_to_remove = set(p2f.indices)

# Calculate remaining information

sampling_set = sampling_set.difference(samplings_to_remove)

for sampling in sampling_set:
    if "_nearest" in sampling:
        k, _ = sampling.split("_")
        k = int(k)
        p2f.calc_k_nearest(k, args.n_jobs)
    elif sampling == "beam":
        p2f.calc_beam(args.n_jobs)
    else:
        raise Exception(f"{sampling} sampling not implemented.")

if len(sampling_set):
    p2f.save_indices(indices_folder)

    print_ok("Indices saved successfully.", args.verbose)

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

pipe = joblib.load(open(model_filepath, "rb"))
y_pred = pipe.predict(X)

print_ok("Predictions made successfully.", args.verbose)

output_path = pathlib.Path(output_path)
output_path.mkdir(parents=True, exist_ok=True)

if args.output_hdf:
    output_filepath = output_path / "predictions.hdf"
else:
    output_filepath = output_path / "predictions.csv"

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

if args.output_hdf:
    predictions.save_hdf(output_filepath)
else:
    # np.savetxt(output_filepath, predictions.predictions, delimiter=",")
    pd.DataFrame(predictions.predictions, columns=["prediction"]).to_csv(
        output_filepath, index=False, delimiter=","
    )
