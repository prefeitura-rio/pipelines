"""
Utilities for `db_dump` tasks.
"""

from pathlib import Path
from typing import Tuple, List, Union

from unidecode import unidecode
import pandas as pd

from pipelines.utils import log

###############
#
# Dataframe
#
###############


def batch_to_dataframe(batch: Tuple[Tuple], columns: List[str]) -> pd.DataFrame:
    """
    Converts a batch of rows to a dataframe.
    """
    log(f"Converting batch of size {len(batch)} to dataframe")
    return pd.DataFrame(batch, columns=columns)


def clean_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    for col in dataframe.columns.tolist():
        if dataframe[col].dtype == object:
            dataframe[col] = dataframe[col].str.replace("\x00", "", regex=True)
    return dataframe


###############
#
# File
#
###############


def dataframe_to_csv(dataframe: pd.DataFrame, path: Union[str, Path]) -> None:
    """
    Writes a dataframe to a CSV file.
    """
    # Remove filename from path
    path = Path(path)
    # Create directory if it doesn't exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Write dataframe to CSV
    log(f"Writing dataframe to CSV: {path}")
    dataframe.to_csv(path, index=False, encoding="utf-8")
    log(f"Wrote dataframe to CSV: {path}")


import os
from pathlib import Path
import pandas as pd


def to_partitions(data, partition_columns, savepath):
    """Save data in to hive patitions schema, given a dataframe and a list of partition columns.
    Args:
        data (pandas.core.frame.DataFrame): Dataframe to be partitioned.
        partition_columns (list): List of columns to be used as partitions.
        savepath (str, pathlib.PosixPath): folder path to save the partitions

    Exemple:

        data = {
            "ano": [2020, 2021, 2020, 2021, 2020, 2021, 2021,2025],
            "mes": [1, 2, 3, 4, 5, 6, 6,9],
            "sigla_uf": ["SP", "SP", "RJ", "RJ", "PR", "PR", "PR","PR"],
            "dado": ["a", "b", "c", "d", "e", "f", "g",'h'],
        }

        to_partitions(
            data=pd.DataFrame(data),
            partition_columns=['ano','mes','sigla_uf'],
            savepath='partitions/'
        )
    """

    if isinstance(data, (pd.core.frame.DataFrame)):

        savepath = Path(savepath)

        unique_combinations = (
            data[partition_columns]
            .drop_duplicates(subset=partition_columns)
            .to_dict(orient="records")
        )

        for filter_combination in unique_combinations:
            patitions_values = [
                f"{partition}={value}"
                for partition, value in filter_combination.items()
            ]
            filter_save_path = Path(savepath / "/".join(patitions_values))
            filter_save_path.mkdir(parents=True, exist_ok=True)

            df_filter = data.loc[
                data[filter_combination.keys()]
                .isin(filter_combination.values())
                .all(axis=1),
                :,
            ]
            df_filter = df_filter.drop(columns=partition_columns)

            df_filter.to_csv(filter_save_path / "data.csv", index=False)

    else:
        raise (BaseException("Data need to be a pandas DataFrame"))
