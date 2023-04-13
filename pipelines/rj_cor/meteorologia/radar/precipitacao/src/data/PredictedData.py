# -*- coding: utf-8 -*-
# Atenção: Informação proprietária. Leia a licença de uso.
# flake8: noqa: E501
# pylint: skip-file
import pathlib

import h5py
import numpy as np
import plotly.graph_objects as go

MAP_CENTER = {"lat": -22.9932804107666, "lon": -43.26795928955078}

# Class for saving model predictions


class PredictedData:
    def __init__(
        self,
        predictions: np.array,
        coordinates: np.array,
        dataset: str,
        model: str,
        date: str,
        time: str,
    ):
        self.predictions = predictions
        self.coordinates = coordinates
        self.dataset = dataset
        self.model = model
        self.date = date
        self.time = time

    @classmethod
    def load_hdf(cls, input_filepath: pathlib.Path):
        with h5py.File(input_filepath, "r") as f:
            return cls(
                np.array(f["predictions"]),
                np.array(f["coordinates"]),
                f.attrs["dataset"],
                f.attrs["model"],
                f.attrs["date"],
                f.attrs["time"],
            )

    def save_hdf(self, output_filepath: pathlib.Path):
        with h5py.File(output_filepath, "w") as f:
            f.create_dataset("predictions", data=self.predictions, compression="gzip")
            f.create_dataset("coordinates", data=self.coordinates, compression="gzip")
            f.attrs["dataset"] = self.dataset
            f.attrs["model"] = self.model
            f.attrs["date"] = self.date
            f.attrs["time"] = self.time

    def write_image(
        self,
        output_filepath: pathlib.Path = None,
        range_color: list = [0, 10],
        lower_bound: float = 0.4,
        colorscale: str = "YlGnBu",
        zoom: float = 6.3,
        opacity: float = 1.0,
        return_fig=False,
        map_center=MAP_CENTER,
        interactive=False,
    ):
        inds_to_keep = self.predictions >= lower_bound
        truncated_predictions = self.predictions[inds_to_keep]
        fig = go.Figure(
            go.Scattermapbox(
                lat=self.coordinates[:, 0][inds_to_keep],
                lon=self.coordinates[:, 1][inds_to_keep],
                mode="markers",
                marker={
                    "cmin": range_color[0],
                    "cmax": range_color[1],
                    "color": truncated_predictions,
                    "colorscale": colorscale,
                    "opacity": opacity,
                    "colorbar": dict(
                        title="Rain (mm in 15 min)",
                    ),
                },
                hovertemplate="Latitude: %{lat:.3f}<br>"
                + "Longitude: %{lon:.3f}<br>"
                + "Rain: %{marker.color:,}"
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
