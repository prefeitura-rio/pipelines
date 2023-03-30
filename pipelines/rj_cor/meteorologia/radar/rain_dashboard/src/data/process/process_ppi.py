# -*- coding: utf-8 -*-
import argparse
import datetime
import pathlib
from multiprocessing.pool import Pool

import h5py
import numpy as np
from tqdm import tqdm

from pipelines.rj_cor.meteorologia.radar.rain_dashboard.src.data.process.RadarData import (
    RSCALE,
    RSTART,
    RadarData,
)
from pipelines.rj_cor.meteorologia.radar.rain_dashboard.src.utils.data_utils import (
    NRAYS,
)
from pipelines.rj_cor.meteorologia.radar.rain_dashboard.src.utils.general_utils import (
    parse_dates_argument,
    print_error,
    print_ok,
    print_warning,
)

# def check_azimuth_sweep(startazA: np.array, stopazA: np.array) -> bool:
#     differences = (stopazA-startazA) % 360

#     return np.where(np.logical_not(np.isclose(differences, 1, rtol=1, atol = 0.1)))


def prj0(x, y):
    return x


def prj1(x, y):
    return y


def partition_consecutive_mod_n(l1: list, n: int):
    if n <= 0:
        raise ValueError("Integer n must be positive.")

    if len(l1) == 0:
        raise ValueError("List is empty.")

    l2 = sorted(l1)

    partition_dict = {l2[0]: [l2[0]]}

    last_partition_representative = l2[0]
    for k in l2[1:]:
        if k >= n:
            raise ValueError(
                "There is a number in list that is greater or equal than n."
            )

        if (k - 1) in partition_dict[last_partition_representative]:
            partition_dict[last_partition_representative].append(k)
        else:
            partition_dict[k] = [k]
            last_partition_representative = k

    if l2[0] == 0 and n - 1 in partition_dict[last_partition_representative]:
        partition_dict[last_partition_representative].extend(partition_dict[l2[0]])
        del partition_dict[l2[0]]

    return partition_dict


def get_azimuth_indices(sweep_info: dict) -> dict:
    indices_dict = dict()

    for primary_key in sweep_info.keys():
        startazA = sweep_info[primary_key]["startazA"]
        stopazA = sweep_info[primary_key]["stopazA"]

        pos_dist = ((stopazA - startazA) % 360) / 2
        neg_dist = 180 - pos_dist

        dist = np.minimum(pos_dist, neg_dist)

        avgazA = (startazA + (2 * (pos_dist < neg_dist) - 1) * dist) % 360
        hist, bins = np.histogram(avgazA, bins=np.arange(0, NRAYS + 1, 1))
        indices_dict[primary_key] = dict()
        indices_dict[primary_key]["repeated_indices"] = dict()

        repeated_indices = np.asarray(hist > 1).nonzero()[0]
        for i in repeated_indices:
            indices_dict[primary_key]["repeated_indices"][i] = []
            for j, az in enumerate(avgazA):
                if bins[i] <= az < bins[i + 1]:
                    indices_dict[primary_key]["repeated_indices"][i].append(j)

        indices_dict[primary_key]["missing_indices"] = np.asarray(hist == 0).nonzero()[
            0
        ]
        indices_dict[primary_key]["indices"] = np.digitize(avgazA, bins) - 1

    return indices_dict


def process_data(full_matrix: np.array, operator) -> dict:
    processed_data, index_matrix = operator(full_matrix)

    return processed_data, index_matrix


# Add to a utils file
VARIABLES_DICT = {
    "TH": "data1",
    "TV": "data2",
    "DBZH": "data3",
    "DBZV": "data4",
    "ZDR": "data5",
    "RHOHV": "data6",
    "PHIDP": "data7",
    "SQI": "data8",
    "SNR": "data9",
    "VRAD": "data10",
    "WRAD": "data11",
}


# flake8: noqa
def process_ppi(
    verbose: bool,
    feature: str,
    process_type: str,
    filepath: str,
    index_matrix: np.array = None,
):
    if feature not in VARIABLES_DICT.keys():
        print_error("Error: Specified feature not allowed.")
        exit()

    load_index_matrix = "_" in process_type
    if load_index_matrix:
        indices_process_type, indices_feature = process_type.split("_")

    possible_process_types = ["CMAX", "CAPPI", "PSEUDO-CAPPI", "CAVG", "RHOHV-CAVG"]

    if (
        (not load_index_matrix and process_type not in possible_process_types)
        or (load_index_matrix and indices_process_type not in possible_process_types)
        or (load_index_matrix and indices_feature not in VARIABLES_DICT.keys())
    ):
        print_error("Error: Specified process type not allowed.")
        exit()

    hdf = h5py.File(filepath)

    lat = hdf["where"].attrs["lat"]
    lon = hdf["where"].attrs["lon"]

    file_date = hdf["what"].attrs["date"].decode("UTF-8")
    time = hdf["what"].attrs["time"].decode("UTF-8")

    gains = []
    offsets = []
    if process_type == "RHOHV-CAVG":
        rhohv_gains = []
        rhohv_offsets = []

    secondary_key = VARIABLES_DICT[feature]
    sweep_info = dict(
        [
            (primary_key, dict())
            for primary_key in hdf.keys()
            if primary_key not in ["how", "what", "where"]
        ]
    )

    dataset_keys = [key for key in hdf.keys() if key not in ["how", "what", "where"]]
    for elevation, primary_key in enumerate(dataset_keys):
        sweep_info[primary_key]["nrays"] = hdf[primary_key]["where"].attrs["nrays"]
        sweep_info[primary_key]["nbins"] = hdf[primary_key]["where"].attrs["nbins"]

        rscale = hdf[primary_key]["where"].attrs["rscale"]
        rstart = hdf[primary_key]["where"].attrs["rstart"]

        assert rstart == RSTART, f"rstart not {RSTART} as expected."
        assert rscale == RSCALE, f"rscale not {RSCALE} as expected."

        sweep_info[primary_key]["startazA"] = np.array(
            hdf[primary_key]["how"].attrs["startazA"]
        )
        sweep_info[primary_key]["stopazA"] = np.array(
            hdf[primary_key]["how"].attrs["stopazA"]
        )
        gains.append(hdf[primary_key][secondary_key]["what"].attrs["gain"])
        offsets.append(hdf[primary_key][secondary_key]["what"].attrs["offset"])
        if process_type == "RHOHV-CAVG":
            rhohv_gains.append(
                hdf[primary_key][VARIABLES_DICT["RHOHV"]]["what"].attrs["gain"]
            )
            rhohv_offsets.append(
                hdf[primary_key][VARIABLES_DICT["RHOHV"]]["what"].attrs["offset"]
            )

    assert gains.count(gains[0]) == len(gains), "Not all gains are identical."
    assert offsets.count(offsets[0]) == len(offsets), "Not all offsets are identical."
    gain = gains[0]
    offset = offsets[0]

    assert gain > 0, "gain variable less or equal than zero."
    del gains
    del offsets
    if process_type == "RHOHV-CAVG":
        assert rhohv_gains.count(rhohv_gains[0]) == len(
            rhohv_gains
        ), "Not all gains are identical."
        assert rhohv_offsets.count(rhohv_offsets[0]) == len(
            rhohv_offsets
        ), "Not all offsets are identical."
        rhohv_gain = rhohv_gains[0]
        rhohv_offset = rhohv_offsets[0]

        assert rhohv_gain > 0, "gain variable less or equal than zero."
        del rhohv_gains
        del rhohv_offsets

    nrays_max = max(
        [sweep_info[primary_key]["nrays"] for primary_key in sweep_info.keys()]
    )
    nbins_max = max(
        [sweep_info[primary_key]["nbins"] for primary_key in sweep_info.keys()]
    )
    assert nrays_max == NRAYS, f"Maximum value of nrays over elevations is not {NRAYS}."
    try:
        assert (
            nbins_max == 1000
        ), f"Maximum value of nbins over elevations is not 1000 in file {filepath}."
    except AssertionError as e:
        print_warning(e, verbose)
    indices_dict = get_azimuth_indices(sweep_info)

    # except AssertionError as e:
    full_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))
    if process_type == "RHOHV-CAVG":
        rhohv_full_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))

    full_startazA_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))
    full_stopazA_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))
    full_startazT_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))
    full_stopazT_matrix = -np.ones((nrays_max, nbins_max, len(dataset_keys)))
    full_elevation_matrix = np.empty((nrays_max, nbins_max, len(dataset_keys)))

    for el_index, primary_key in enumerate(dataset_keys):
        if len(indices_dict[primary_key]["missing_indices"]) > 0:
            print_warning(
                f"Warning: missing indices in {filepath} at {primary_key}.", verbose
            )
        if len(indices_dict[primary_key]["repeated_indices"].keys()) > 0:
            print_warning(
                f"Warning: repeated indices in {filepath} at {primary_key}.", verbose
            )

        data_array = np.array(hdf[primary_key][secondary_key]["data"])

        if process_type == "RHOHV-CAVG":
            rhohv_data_array = np.array(
                hdf[primary_key][VARIABLES_DICT["RHOHV"]]["data"]
            )

        for i in indices_dict[primary_key]["repeated_indices"].keys():
            repeated_list = indices_dict[primary_key]["repeated_indices"][i]

            nonzero_repeated_list = repeated_list

            zero_line_indices = np.where(
                np.all(data_array[repeated_list, :] == 0, axis=1)
            )[0]

            succeding_ray_index = (i + 1) % NRAYS
            while succeding_ray_index in indices_dict[primary_key]["missing_indices"]:
                succeding_ray_index = (succeding_ray_index + 1) % NRAYS
            preceding_ray_index = (i - 1) % NRAYS
            while preceding_ray_index in indices_dict[primary_key]["missing_indices"]:
                preceding_ray_index = (preceding_ray_index - 1) % NRAYS

            # Warn when a whole repeated line is zero. If this is the case, take the mean of non-zero repeated lines
            if len(zero_line_indices):
                print_warning(
                    f"Warning: Data array at one of the repeated indices in {filepath} at {primary_key} is all zeros.",
                    verbose,
                )
                nonzero_repeated_list = list(
                    set(repeated_list)
                    - set([repeated_list[ind] for ind in zero_line_indices])
                )

            nonzero_repeated_list += [
                list(indices_dict[primary_key]["indices"]).index(preceding_ray_index),
                list(indices_dict[primary_key]["indices"]).index(succeding_ray_index),
            ]

            data_array[repeated_list, :] = np.median(
                data_array[list(nonzero_repeated_list), :], axis=0
            ).reshape((1, data_array.shape[1]))

        startazA = np.array(hdf[primary_key]["how"].attrs["startazA"])
        stopazA = np.array(hdf[primary_key]["how"].attrs["stopazA"])
        startazT = np.array(hdf[primary_key]["how"].attrs["startazT"])
        stopazT = np.array(hdf[primary_key]["how"].attrs["stopazT"])
        full_matrix[
            indices_dict[primary_key]["indices"],
            : hdf[primary_key]["where"].attrs["nbins"],
            el_index,
        ] = data_array
        if process_type == "RHOHV-CAVG":
            rhohv_full_matrix[
                indices_dict[primary_key]["indices"],
                : hdf[primary_key]["where"].attrs["nbins"],
                el_index,
            ] = rhohv_data_array
        full_startazA_matrix[
            indices_dict[primary_key]["indices"], :, el_index
        ] = startazA.reshape(startazA.shape[0], 1)
        full_stopazA_matrix[
            indices_dict[primary_key]["indices"], :, el_index
        ] = stopazA.reshape(stopazA.shape[0], 1)
        full_startazT_matrix[
            indices_dict[primary_key]["indices"], :, el_index
        ] = startazT.reshape(startazT.shape[0], 1)
        full_stopazT_matrix[
            indices_dict[primary_key]["indices"], :, el_index
        ] = stopazT.reshape(stopazT.shape[0], 1)

        full_elevation_matrix[:, :, el_index] = hdf[primary_key]["where"].attrs[
            "elangle"
        ]

        missing_indices_partition = None
        try:
            missing_indices_partition = partition_consecutive_mod_n(
                indices_dict[primary_key]["missing_indices"], NRAYS
            )
        except ValueError:
            missing_indices_partition = {}

        for class_representative in missing_indices_partition.keys():
            m = len(missing_indices_partition[class_representative])
            preceding_ray_index = (class_representative - 1) % NRAYS
            succeding_ray_index = (
                missing_indices_partition[class_representative][-1] + 1
            ) % NRAYS

            for i, ind in enumerate(missing_indices_partition[class_representative]):
                full_matrix[ind, :, el_index] = np.average(
                    full_matrix[
                        [preceding_ray_index, succeding_ray_index], :, el_index
                    ],
                    axis=0,
                    weights=[(i + 1) / (m + 1), 1 - (i + 1) / (m + 1)],
                ).reshape((1, nbins_max))

                if process_type == "RHOHV-CAVG":
                    rhohv_full_matrix[ind, :, el_index] = np.zeros((1, nbins_max))
    if process_type == "RHOHV-CAVG":
        rhohv_full_matrix = rhohv_full_matrix * rhohv_gain + rhohv_offset
    ind0 = np.fromfunction(prj0, (nrays_max, nbins_max)).astype(int)
    ind1 = np.fromfunction(prj1, (nrays_max, nbins_max)).astype(int)

    operator = None
    if load_index_matrix:

        def operator(full_matrix):
            def from_list(x):
                return x[0]

            indices = np.vectorize(from_list)(index_matrix)
            return full_matrix[ind0, ind1, indices], index_matrix

    elif process_type == "CMAX":

        def operator(full_matrix):
            index_matrix = np.argmax(full_matrix, axis=2)

            def to_list(x):
                return [x]

            return full_matrix[ind0, ind1, index_matrix], np.vectorize(
                to_list, otypes=["object"]
            )(index_matrix)

    # elif process_type=='CAPPI':
    elif process_type == "PSEUDO-CAPPI":
        ranges = (rstart + rscale / 2) + np.arange(0, nbins_max) * rscale
        elevations = full_elevation_matrix[0, 0, :]

        assert np.all(elevations < 90), "Unexpected value for elevation angles."

        altitude = 1500
        elevations_per_range = (np.arctan(altitude / ranges) * 180 / np.pi).flatten()
        digitized = np.digitize(elevations_per_range, elevations) - 1
        down_elevations = np.asarray(digitized == -1).nonzero()[0]
        up_elevations = np.asarray(digitized == len(elevations) - 1).nonzero()[0]
        mid_elevations = np.asarray(
            np.logical_and(digitized >= 0, digitized < len(elevations) - 1)
        ).nonzero()[0]

        def to_list(x):
            return [x]

        def with_successor_to_list(x):
            return [x, x + 1]

        v_to_list = np.vectorize(to_list, otypes=["object"])
        v_with_successor_to_list = np.vectorize(
            with_successor_to_list, otypes=["object"]
        )
        elevation_indices_per_range = np.hstack(
            [
                v_to_list(digitized[up_elevations]),
                v_with_successor_to_list(digitized[mid_elevations]),
                v_to_list(digitized[down_elevations]),
            ]
        )
        mid_elevations_ratios = (
            elevations_per_range[mid_elevations] - elevations[digitized[mid_elevations]]
        ) / (
            elevations[digitized[mid_elevations] + 1]
            - elevations[digitized[mid_elevations]]
        )
        weights_matrix = np.empty((nrays_max, len(mid_elevations), 2))
        weights_matrix[::] = np.vstack(
            [mid_elevations_ratios, 1 - mid_elevations_ratios]
        ).T.reshape(1, -1, 2)
        down_indices_matrix = np.empty((nrays_max, len(down_elevations)), dtype="int")
        up_indices_matrix = np.empty((nrays_max, len(up_elevations)), dtype="int")

        down_indices_matrix[:, :] = digitized[down_elevations].reshape(1, -1)
        up_indices_matrix[:, :] = digitized[up_elevations].reshape(1, -1)
        mid_indices_matrix = np.empty(
            (nrays_max, len(mid_elevations_ratios), 2), dtype="int"
        )

        mid_indices_matrix[:] = np.array(
            list(v_with_successor_to_list(digitized[mid_elevations])), dtype="int"
        )

        def operator(full_matrix):
            index_matrix = np.empty((nrays_max, nbins_max), dtype="object")
            index_matrix[:] = elevation_indices_per_range.reshape(1, -1)

            ind0up = np.fromfunction(prj0, (nrays_max, len(up_elevations))).astype(int)
            ind1up = np.fromfunction(prj1, (nrays_max, len(up_elevations))).astype(int)

            def prj0mid(x, y, z):
                return x

            def prj1mid(x, y, z):
                return y

            ind0mid = np.fromfunction(
                prj0mid, (nrays_max, len(mid_elevations), 2)
            ).astype(int)
            ind1mid = np.fromfunction(
                prj1mid, (nrays_max, len(mid_elevations), 2)
            ).astype(int)

            ind0down = np.fromfunction(prj0, (nrays_max, len(down_elevations))).astype(
                int
            )
            ind1down = np.fromfunction(prj1, (nrays_max, len(down_elevations))).astype(
                int
            )
            processed_data_matrix = np.hstack(
                [
                    full_matrix[:, 0 : len(up_elevations), :][
                        ind0up, ind1up, up_indices_matrix
                    ],
                    np.average(
                        full_matrix[
                            :,
                            len(up_elevations) : len(up_elevations)
                            + len(mid_elevations),
                            :,
                        ][ind0mid, ind1mid, mid_indices_matrix],
                        axis=2,
                        weights=weights_matrix,
                    ),
                    full_matrix[:, 0 : len(down_elevations), :][
                        ind0down, ind1down, down_indices_matrix
                    ],
                ]
            )

            return processed_data_matrix, index_matrix

    elif process_type == "CAVG":

        def operator(full_matrix):
            index_matrix = np.empty(shape=(nrays_max, nbins_max), dtype="object")
            for i in range(nrays_max):
                for j in range(nbins_max):
                    index_matrix[i][j] = list(np.where(full_matrix[i, j, :] != -1)[0])

            nan_full_matrix = np.where(full_matrix != -1, full_matrix, np.nan)
            processed_data_matrix = np.nanmean(nan_full_matrix, axis=2)

            return processed_data_matrix, index_matrix

    elif process_type == "RHOHV-CAVG":

        def operator(full_matrix):
            index_matrix = np.empty(shape=(nrays_max, nbins_max), dtype="object")
            for i in range(nrays_max):
                for j in range(nbins_max):
                    index_matrix[i][j] = list(np.where(full_matrix[i, j, :] != -1)[0])

            nan_full_matrix = np.where(full_matrix != -1, full_matrix, np.nan)
            nan_rhohv_full_matrix = np.where(
                rhohv_full_matrix != -1, rhohv_full_matrix, np.nan
            )

            nan_rhohv_full_matrix = np.where(
                np.isnan(nan_full_matrix), np.nan, nan_rhohv_full_matrix
            )
            nan_full_matrix = np.where(
                np.isnan(nan_rhohv_full_matrix), np.nan, nan_full_matrix
            )

            np.nansum(nan_rhohv_full_matrix, axis=2)
            processed_data_matrix = np.nansum(
                nan_full_matrix * nan_rhohv_full_matrix, axis=2
            ) / np.nansum(nan_rhohv_full_matrix, axis=2)
            processed_data_matrix = np.where(
                np.isnan(processed_data_matrix), 0, processed_data_matrix
            )

            return processed_data_matrix, index_matrix

    else:
        print_error("Processing type not yet implemented.")
        exit()

    processed_data, index_matrix = process_data(full_matrix, operator)

    def get_from_indices(data, indices):
        new_data = np.empty((nrays_max, nbins_max), dtype="object")
        index_permutations = (
            (n % nrays_max, int(n / nrays_max)) for n in range(nrays_max * nbins_max)
        )
        for i, j in index_permutations:
            new_data[i, j] = data[i, j, indices[i, j]]
        return new_data

    startazA_matrix = get_from_indices(full_startazA_matrix, index_matrix)
    stopazA_matrix = get_from_indices(full_stopazA_matrix, index_matrix)
    startazT_matrix = get_from_indices(full_startazT_matrix, index_matrix)
    stopazT_matrix = get_from_indices(full_stopazT_matrix, index_matrix)
    elevation_matrix = get_from_indices(full_elevation_matrix, index_matrix)

    radar_data = RadarData(
        processed_data,
        process_type,
        gain,
        offset,
        feature,
        nrays_max,
        nbins_max,
        index_matrix,
        startazA_matrix,
        stopazA_matrix,
        startazT_matrix,
        stopazT_matrix,
        elevation_matrix,
        file_date,
        time,
        lat,
        lon,
    )
    return radar_data
