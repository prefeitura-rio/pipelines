# -*- coding: utf-8 -*-
# Attention: This file is licensed through the EULA license present in the license file at the root
# of this repository. Copying any part of this code is prohibited.
# flake8: noqa: E501
# pylint: skip-file
import argparse
import pathlib

import h5py
import numpy as np

from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.data_utils import (
    NRAYS,
)
from pipelines.rj_cor.meteorologia.radar.precipitacao.src.utils.general_utils import (
    print_error,
    print_ok,
    print_warning,
)


def main(path_list, verbose=True):
    filepaths = []

    # Save constants here
    RADAR_LATITUDE = -22.9932804107666
    RADAR_LONGITUDE = -43.58795928955078

    # Obtain all files associated with the input
    # You may input files and folders

    if isinstance(path_list, str):
        filepaths = [pathlib.Path(path_list)]
    else:
        for path_string in path_list:
            path = pathlib.Path(path_string)
            if path.is_dir():
                filepaths.extend(path.glob("**/*.*"))
            elif path.is_file():
                filepaths.append(path)
            else:
                print_error(f"{path} is neither a directory nor a file.")
                exit()

    # HDF structure is given by hdf[primary_key][secondary_keys]
    # The primary key indicates the elevation angle of the radar, while the secondary key indicates the product
    # The variables dict can be used to find which product is associated with each entry

    primary_keys = [
        "dataset1",
        "dataset2",
        "dataset3",
        "dataset4",
        "dataset5",
        "dataset6",
        "dataset7",
        "how",
        "what",
        "where",
    ]
    secondary_keys = [
        "data1",
        "data10",
        "data11",
        "data2",
        "data3",
        "data4",
        "data5",
        "data6",
        "data7",
        "data8",
        "data9",
        "how",
        "what",
        "where",
    ]
    variables_dict = {
        "data1": "TH",
        "data10": "VRAD",
        "data11": "WRAD",
        "data2": "TV",
        "data3": "DBZH",
        "data4": "DBZV",
        "data5": "ZDR",
        "data6": "RHOHV",
        "data7": "PHIDP",
        "data8": "SQI",
        "data9": "SNR",
    }

    # Iterate over all given files
    for filepath in filepaths:
        locator = f"{filepath}: "

        assert filepath.suffix == ".hdf", locator + "Filepath extension is not .hdf."
        hdf = h5py.File(filepath)

        # Check if latitude and longitude is close to where the radar is located
        assert np.isclose(hdf["where"].attrs["lat"], RADAR_LATITUDE), (
            locator + "Radar latitude is inconsistent."
        )
        assert np.isclose(hdf["where"].attrs["lon"], RADAR_LONGITUDE), (
            locator + "Radar longitude is inconsistent."
        )

        # Date and time should be the same as those indicated in file name
        assert f"-{hdf['what'].attrs['date'].decode('UTF-8')}-" in filepath.name, (
            locator + "Date in metadata different from date in file name."
        )
        assert f"-{hdf['what'].attrs['time'].decode('UTF-8')}-" in filepath.name, (
            locator + "Time in metadata different from time in file name."
        )

        # All processed files must be PPI files
        assert hdf["what"].attrs["object"].decode("UTF-8") == "PVOL", (
            locator + "File is not PPIVol."
        )

        # Verify the list of keys is the same as the expected primary keys (i.e. no elevation has been added or is missing)
        assert list(hdf.keys()) == primary_keys, (
            locator + "File primary keys are not as expected."
        )

        for primary_key in hdf.keys():
            if primary_key in ["how", "what", "where"]:
                continue

            locator = f"{filepath}/{primary_key}: "
            assert list(hdf[primary_key].keys()) == secondary_keys, (
                locator + "File secondary keys are not as expected."
            )

            # Verify parameters indicating grid structure
            nbins = hdf[primary_key]["where"].attrs["nbins"]
            nrays = hdf[primary_key]["where"].attrs["nrays"]
            rscale = hdf[primary_key]["where"].attrs["rscale"]
            rstart = hdf[primary_key]["where"].attrs["rstart"]

            assert isinstance(nbins, (np.integer, int)) and nbins > 0, (
                locator + "nbins variable is not a positive integer."
            )
            assert isinstance(nrays, (np.integer, int)) and nrays > 0, (
                locator + "nrays variable is not a positive integer."
            )
            assert isinstance(rscale, (np.floating, float)) and rscale > 0, (
                locator + "rscale variable is not a positive float."
            )
            assert isinstance(rstart, (np.floating, float)) and rstart >= 0, (
                locator + "rstart variable is not a non negative float."
            )

            # Verify if azimuth array has the right number of rays
            startazA = np.array(hdf[primary_key]["how"].attrs["startazA"])
            stopazA = np.array(hdf[primary_key]["how"].attrs["stopazA"])

            assert startazA.shape == (nrays,), (
                locator + "Array of start azimuth angles has inconsistent shape."
            )
            assert stopazA.shape == (nrays,), (
                locator + "Array of stop azimuth angles has inconsistent shape."
            )

            assert np.all(startazA >= 0) and np.all(startazA < NRAYS), (
                locator + "Values for start azimuth angles are invalid."
            )
            assert np.all(stopazA >= 0) and np.all(stopazA < NRAYS), (
                locator + "Values for stop azimuth angles are invalid."
            )

            # Ensure data is not simulated data
            assert (
                hdf[primary_key]["how"].attrs["simulated"].decode("UTF-8") == "False"
            ), (locator + "Input data is simulated.")

            for secondary_key in hdf[primary_key].keys():
                if secondary_key in ["how", "what", "where"]:
                    continue

                locator = f"{filepath}/{primary_key}/{secondary_key}: "

                # Verify if variable names are correct
                assert (
                    hdf[primary_key][secondary_key]["what"]
                    .attrs["quantity"]
                    .decode("UTF-8")
                    == variables_dict[secondary_key]
                ), (locator + "Unexpected variable name.")

                # For each product within secondary keys, should only contain data and its description
                assert list(hdf[primary_key][secondary_key].keys()) == [
                    "data",
                    "what",
                ], (
                    locator + "Data variable keys not expected."
                )

                # Verify if gain and offset parameters are appropriate
                gain = hdf[primary_key][secondary_key]["what"].attrs["gain"]
                offset = hdf[primary_key][secondary_key]["what"].attrs["offset"]

                assert isinstance(gain, (np.floating, float)), (
                    locator + "gain variable is not a float."
                )
                assert isinstance(offset, (np.floating, float)), (
                    locator + "offset variable is not a float."
                )

                # Obtain data array and ensure its size is consistent
                data_array = np.array(hdf[primary_key][secondary_key]["data"])

                assert data_array.shape == (nrays, nbins), (
                    locator
                    + "Data array shape is inconsistent with nrays and nbins variables."
                )

                # Data should be non-negative, where NAs = 0
                assert np.all(data_array >= 0), (
                    locator + "Not all entries of data array are non-negative."
                )
                assert not np.all(data_array == 0), (
                    locator + "All entries of data array are zero."
                )

        print_ok(f"{str(filepath)} passed through all tests successfully.", verbose)
    if len(filepaths) > 1:
        print_ok("All files passed through all tests successfully.", verbose)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "paths",
        nargs="+",
        help="Files passed are checked. May be passed as a directory, so every .hdf in the directory and in subdirectories will be checked or as a list of filepaths.",
        type=str,
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="If true, prints verbose output."
    )

    args = parser.parse_args()
    main(args.paths, args.verbose)
