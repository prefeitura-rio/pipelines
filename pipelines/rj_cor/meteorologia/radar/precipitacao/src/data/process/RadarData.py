# -*- coding: utf-8 -*-
# Atenção: Informação proprietária. Leia a licença de uso.
# flake8: noqa: E501
# pylint: skip-file
import pathlib

import h5py
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyproj

from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.data_utils import (
    NRAYS,
)

RSCALE = 250.0
RSTART = 0.0

MAP_CENTER = {"lat": -22.9932804107666, "lon": -43.26795928955078}

LOG_SCALE_VARIABLES = ["TH", "TV", "DBZH", "DBZV", "ZDR"]


def flat_concat(v: np.array):
    if v is None:
        return h5py.Empty("f")
    flat_v = v.flatten()
    flat_concat_v = []
    for i in range(len(flat_v)):
        try:
            flat_concat_v += list(flat_v[i])
        except TypeError:
            flat_concat_v += [flat_v[i]]
    return flat_concat_v


def invert_flat_concat(
    v: np.array, list_lengths_cumsum: np.array, nrays: int, nbins: int
):
    if v.shape is None:
        return None
    v_split = np.array(np.split(v, list_lengths_cumsum), dtype="object")
    return v_split.reshape(nrays, nbins)


class RadarData:
    def __init__(
        self,
        data: np.array,
        process_type: str,
        gain: float,
        offset: float,
        feature: str,
        nrays: int,
        nbins: int,
        indices: np.array,
        startazA: np.array,
        stopazA: np.array,
        startazT: np.array,
        stopazT: np.array,
        elevations: np.array,
        date: str,
        time: str,
        lat: float,
        lon: float,
        compressed: bool = True,
    ):
        self.data = data
        self.process_type = process_type
        self.gain = gain
        self.offset = offset
        self.feature = feature

        assert nrays == NRAYS, f"nrays should be {NRAYS}"
        self.nrays = nrays
        self.nbins = nbins
        self.indices = indices
        self.startazA = startazA
        self.stopazA = stopazA
        self.startazT = startazT
        self.stopazT = stopazT
        self.elevations = elevations
        self.date = date
        self.time = time
        self.lat = lat
        self.lon = lon
        self.compressed = compressed

    @classmethod
    def load_hdf(cls, input_filepath: pathlib.Path):
        with h5py.File(input_filepath, "r") as f:
            if f["list_lengths"].shape is None:
                list_lengths_cumsum = None
            else:
                list_lengths_cumsum = np.cumsum(f["list_lengths"])[:-1]
            nrays = f.attrs["nrays"]
            nbins = f.attrs["nbins"]
            indices = invert_flat_concat(
                f["indices"], list_lengths_cumsum, nrays, nbins
            )
            startazA = invert_flat_concat(
                f["startazA"], list_lengths_cumsum, nrays, nbins
            )
            stopazA = invert_flat_concat(
                f["stopazA"], list_lengths_cumsum, nrays, nbins
            )
            startazT = invert_flat_concat(
                f["startazT"], list_lengths_cumsum, nrays, nbins
            )
            stopazT = invert_flat_concat(
                f["stopazT"], list_lengths_cumsum, nrays, nbins
            )
            elevations = invert_flat_concat(
                f["elevations"], list_lengths_cumsum, nrays, nbins
            )

            if "compressed" in f.attrs.keys():
                compressed = f.attrs["compressed"]
            else:
                compressed = True

            return cls(
                np.array(f["dataset"]),
                f.attrs["process_type"],
                f.attrs["gain"],
                f.attrs["offset"],
                f.attrs["feature"],
                nrays,
                nbins,
                indices,
                startazA,
                stopazA,
                startazT,
                stopazT,
                elevations,
                f.attrs["date"],
                f.attrs["time"],
                f.attrs["lat"],
                f.attrs["lon"],
                compressed,
            )

    def save_hdf(self, output_filepath: pathlib.Path):
        flat_indices = flat_concat(self.indices)
        flat_startazA = flat_concat(self.startazA)
        flat_stopazA = flat_concat(self.stopazA)
        flat_startazT = flat_concat(self.startazT)
        flat_stopazT = flat_concat(self.stopazT)
        flat_elevations = flat_concat(self.elevations)
        if self.indices is None:
            list_lengths = h5py.Empty("f")
        else:
            try:
                list_lengths = np.vectorize(len)(self.indices.flatten())
            except TypeError:
                list_lengths = np.ones(self.indices.flatten().shape, dtype=np.int8)

        with h5py.File(output_filepath, "w") as f:
            f.create_dataset("dataset", data=self.data, compression="gzip")
            try:
                f.create_dataset("indices", data=flat_indices, compression="gzip")
                f.create_dataset("startazA", data=flat_startazA, compression="gzip")
                f.create_dataset("stopazA", data=flat_stopazA, compression="gzip")
                f.create_dataset("startazT", data=flat_startazT, compression="gzip")
                f.create_dataset("stopazT", data=flat_stopazT, compression="gzip")
                f.create_dataset("elevations", data=flat_elevations, compression="gzip")
                f.create_dataset("list_lengths", data=list_lengths, compression="gzip")
            except TypeError:
                f.create_dataset("indices", data=flat_indices)
                f.create_dataset("startazA", data=flat_startazA)
                f.create_dataset("stopazA", data=flat_stopazA)
                f.create_dataset("startazT", data=flat_startazT)
                f.create_dataset("stopazT", data=flat_stopazT)
                f.create_dataset("elevations", data=flat_elevations)
                f.create_dataset("list_lengths", data=list_lengths)
            f.attrs["process_type"] = self.process_type
            f.attrs["gain"] = self.gain
            f.attrs["offset"] = self.offset
            f.attrs["feature"] = self.feature
            f.attrs["nrays"] = self.nrays
            f.attrs["nbins"] = self.nbins
            f.attrs["date"] = self.date
            f.attrs["time"] = self.time
            f.attrs["lat"] = self.lat
            f.attrs["lon"] = self.lon
            f.attrs["compressed"] = self.compressed

    def unwrap_data(self):
        if not self.compressed:
            raise ValueError("Radar data is already unwrapped.")

        real_data = np.array(self.data)
        if self.offset != 0:
            real_data[real_data <= 0] = np.nan
        real_data = real_data * self.gain + self.offset

        if self.feature in LOG_SCALE_VARIABLES:
            real_data = 10 ** (real_data / 10)
            real_data = np.nan_to_num(real_data)

        return RadarData(
            real_data,
            self.process_type,
            self.gain,
            self.offset,
            self.feature,
            self.nrays,
            self.nbins,
            self.indices,
            self.startazA,
            self.stopazA,
            self.startazT,
            self.stopazT,
            self.elevations,
            self.date,
            self.time,
            self.lat,
            self.lon,
            False,
        )

    def compress_data(self):
        if self.compressed:
            raise ValueError("Radar data is already compressed.")

        compressed_data = np.array(self.data)

        if self.feature in LOG_SCALE_VARIABLES:
            compressed_data = 10 * np.log10(compressed_data)
            compressed_data = np.where(
                np.isinf(compressed_data), np.nan, compressed_data
            )
        compressed_data[np.isnan(compressed_data)] = self.offset
        compressed_data = (compressed_data - self.offset) / self.gain

        return RadarData(
            compressed_data,
            self.process_type,
            self.gain,
            self.offset,
            self.feature,
            self.nrays,
            self.nbins,
            self.indices,
            self.startazA,
            self.stopazA,
            self.startazT,
            self.stopazT,
            self.elevations,
            self.date,
            self.time,
            self.lat,
            self.lon,
            True,
        )

    def write_image(
        self,
        output_filepath: pathlib.Path = None,
        lower_bound: float = 0,
        range_color: list = [0, 60],
        colorscale: str = "rainbow",
        zoom: float = 6.3,
        opacity: float = 0.3,
        return_fig=False,
        map_center=MAP_CENTER,
        interactive=False,
    ):
        geodesic = pyproj.Geod(ellps="WGS84")
        radii = (RSTART + RSCALE / 2) + np.arange(0, self.nbins) * RSCALE
        azimuths = range(NRAYS)
        data = np.empty((self.nrays, self.nbins))
        if self.compressed:
            data = self.data * self.gain + self.offset
        else:
            data = self.data
        df = []
        for i, azimuth in enumerate(azimuths):
            for j, radius in enumerate(radii):
                data_point = data[i, j]
                if (
                    self.compressed
                    and (data_point <= self.offset or data_point < lower_bound)
                    or (not self.compressed)
                    and (np.isnan(data_point) or data_point < lower_bound)
                ):
                    continue
                lon, lat = geodesic.fwd(self.lon, self.lat, azimuth, radius)[:2]
                df.append((lat, lon, data_point))
        df = pd.DataFrame(df, columns=["latitude", "longitude", self.feature])

        fig = go.Figure(
            go.Scattermapbox(
                lat=df.latitude,
                lon=df.longitude,
                mode="markers",
                marker={
                    "cmin": range_color[0],
                    "cmax": range_color[1],
                    "color": df[self.feature],
                    "colorscale": colorscale,
                    "opacity": opacity,
                    "colorbar": dict(
                        title=self.feature,
                    ),
                },
                hovertemplate="Latitude: %{lat:.3f}<br>"
                + "Longitude: %{lon:.3f}<br>"
                + f"{self.feature}: %{{marker.color:,}}"
                + "<extra></extra>",
            )
        )
        if interactive:
            fig.update_layout(
                margin=dict(l=20, r=20, t=20, b=20), height=600, hovermode="x unified"
            )
        else:
            fig.update_layout(margin=dict(l=20, r=20, t=20, b=20), height=600)
        fig.update_mapboxes(center=map_center, zoom=zoom, style="stamen-terrain")

        if output_filepath is not None:
            if interactive:
                fig.write_html(output_filepath)
            else:
                fig.write_image(output_filepath)
        if return_fig:
            return fig

        raise Exception(
            "No path was passed to save. Pass 'return_fig=True' to return the fig object without saving."
        )
