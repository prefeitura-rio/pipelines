# -*- coding: utf-8 -*-
# Attention: This file is licensed through the EULA license present in the license file at the root
# of this repository. Copying any part of this code is prohibited.
# flake8: noqa: E501
# pylint: skip-file
import pathlib
from functools import partial
from multiprocessing.pool import Pool

import numpy as np
import pyproj

from pipelines.rj_cor.meteorologia.radar.precipitacao.src.data.process.RadarData import (
    LOG_SCALE_VARIABLES,
    RadarData,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.data_utils import (
    NRAYS,
    distance,
    get_close_azimuthal_grid_points,
    get_containing_square,
    get_k_nearest_directed_grid_neighbors,
    get_k_nearest_grid_neighbors,
    get_latlon,
)

RSCALE = 250.0

RSTART = 125.0

REF_LATLON = (-22.9932804107666, -43.58795928955078)

MAP_CENTER = {"lat": -22.9932804107666, "lon": -43.26795928955078}


def get_k_nearest_grid_neighbors_vec(point_latlon: tuple, ref_latlon: tuple, k: int):
    return get_k_nearest_grid_neighbors(ref_latlon, point_latlon, k)


def nearest_neighbors_task(chunk: np.array, k: int):
    return np.apply_along_axis(
        get_k_nearest_grid_neighbors_vec, axis=1, arr=chunk, ref_latlon=REF_LATLON, k=k
    )


MAX_NBINS = 1000


class PointsToFeatures:
    def __init__(
        self, coordinates: np.array, distances: np.array = None, indices: dict = None
    ):
        self.coordinates = coordinates
        if indices is None:
            self.indices = dict()
        else:
            self.indices = indices

        self.n_points = coordinates.shape[0]
        self.pseudo_nbins = MAX_NBINS

        if distances is None:
            geodesic = pyproj.Geod(ellps="WGS84")

            lats = coordinates[:, 0]
            lons = coordinates[:, 1]

            ref_lat = np.array([REF_LATLON[0]] * len(lats))
            ref_lon = np.array([REF_LATLON[1]] * len(lats))

            self.distances = geodesic.inv(ref_lon, ref_lat, lons, lats)[2]

            max_dist = np.max(self.distances)

            assert max_dist < MAX_NBINS * 250 + 125, "Points out of radar range."
        else:
            self.distances = distances

    def calc_k_nearest(self, k: int, n_jobs: int = 1):
        n_jobs = min(n_jobs, self.coordinates.shape[0])
        split_matrix = np.array_split(self.coordinates, n_jobs)

        with Pool(n_jobs) as pool:
            result = np.vstack(
                pool.imap(partial(nearest_neighbors_task, k=k), split_matrix)
            )

        result = np.flip(result, 2)

        neighbors_ind = np.ravel_multi_index(
            result.reshape(self.n_points * k, 2).T, (self.pseudo_nbins, NRAYS)
        ).reshape(self.n_points, k)

        self.indices[f"{k}_nearest"] = neighbors_ind

    def calc_beam(self, n_jobs: int = 1):
        n_jobs = min(n_jobs, self.coordinates.shape[0])
        split_matrix = np.array_split(self.coordinates, n_jobs)

        with Pool(n_jobs) as pool:
            result = np.vstack(
                pool.imap(partial(nearest_neighbors_task, k=1), split_matrix)
            )

        result = result.reshape(self.n_points, 2)

        i_coordinates = result[:, 0]
        j_coordinates = result[:, 1]
        beam_matrix = np.empty(shape=(self.n_points, self.pseudo_nbins, 2), dtype=int)
        beam_matrix[:, :, 0] = i_coordinates.reshape(-1, 1)
        beam_matrix[:, :, 1] = np.arange(self.pseudo_nbins).reshape(1, -1)

        j_coordinates_matrix = np.hstack(
            [j_coordinates.reshape(-1, 1)] * self.pseudo_nbins
        )

        beam_matrix = np.where(
            beam_matrix[:, :, 1] <= j_coordinates_matrix,
            beam_matrix[:, :, 1] * NRAYS + beam_matrix[:, :, 0],
            -1,
        )

        self.indices["beam"] = beam_matrix

    def get_dist(self):
        return self.distances.reshape(-1, 1)

    def apply_nearest_neighbors(self, rd: RadarData, operation: str, k: int):
        assert not rd.compressed, "Passed RadarData instance is compressed"
        data = rd.data
        indices = self.indices[f"{k}_nearest"]
        values = np.take(data.T, indices)

        if operation == "":
            return values
        elif operation == "mean":
            return np.nanmean(values, axis=1).reshape(-1, 1)
        elif operation == "std":
            return np.nanstd(values, axis=1).reshape(-1, 1)
        else:
            raise Exception(f"{operation} operation not implemented yet.")

    def apply_beam(self, rd: RadarData, operation: str, threshold=6.0):
        assert not rd.compressed, "Passed RadarData instance is compressed"

        data = np.hstack([rd.data, np.nan * np.ones((rd.data.shape[0], 1))])
        indices = self.indices["beam"]
        values = np.take(data.T, indices)

        if operation == "accumulated":
            return np.nansum(values, axis=1).reshape(-1, 1)
        elif operation == "rain_bins":
            return np.count_nonzero(values >= threshold, axis=1).reshape(-1, 1)
        elif operation == "dist_last_rain_bin":
            rain_bin_indices = np.where(values >= threshold, indices, -1)
            last_rain_bin_indices = np.max(rain_bin_indices, axis=1)
            last_rain_bin_j = last_rain_bin_indices // NRAYS
            distances = np.where(
                last_rain_bin_indices > -1,
                RSTART + RSCALE * last_rain_bin_j,
                RSTART + RSCALE * (100 * MAX_NBINS),
            )

            return distances.reshape(-1, 1)
        else:
            raise Exception(f"{operation} operation not implemented yet.")

    def save_indices(self, output_path: pathlib.Path):
        assert (
            len(self.indices.keys()) > 0
        ), "There are no calculated indices! Please, run some 'calc_' function first."

        coordinates_output_filepath = pathlib.Path(output_path)
        coordinates_output_filepath.mkdir(parents=True, exist_ok=True)
        coordinates_output_filepath = coordinates_output_filepath / "coordinates.npy"
        np.save(coordinates_output_filepath, self.coordinates)

        dists_output_filepath = pathlib.Path(output_path) / "distances.npy"
        np.save(dists_output_filepath, self.distances)
        for indices_name in self.indices.keys():
            indices_output_filepath = (
                pathlib.Path(output_path) / f"{indices_name}_indices.npy"
            )
            np.save(indices_output_filepath, self.indices[indices_name])

    @classmethod
    def load_indices(cls, input_path: pathlib.Path):
        coordinates_input_filepath = pathlib.Path(f"{input_path}/coordinates.npy")
        loaded_coordinates = np.load(coordinates_input_filepath)
        distances_input_filepath = pathlib.Path(f"{input_path}/distances.npy")
        loaded_distances = np.load(distances_input_filepath)

        loaded_indices = dict()

        indices_paths = pathlib.Path(input_path).rglob("*_indices.npy")
        for path in indices_paths:
            file_name = str(path.stem)
            indices_name = file_name.strip("_indices.npy")
            loaded_indices[indices_name] = np.load(path)

        p2f = cls(loaded_coordinates, loaded_distances, loaded_indices)
        return p2f
