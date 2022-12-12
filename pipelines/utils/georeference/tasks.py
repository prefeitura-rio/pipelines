# -*- coding: utf-8 -*-
"""
Tasks for georeferencing tables
"""

from pathlib import Path
from time import time
from typing import List, Tuple
from uuid import uuid4

import basedosdados as bd
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from geopy.location import Location
import pandas as pd
from prefect import task

from pipelines.utils.georeference.utils import check_if_belongs_to_rio
from pipelines.utils.utils import log


@task
def validate_georeference_mode(mode: str) -> None:
    """
    Validates georeference mode
    """
    if mode not in [
        "distinct",
        # insert new modes here
    ]:
        raise ValueError(
            f"Invalid georeference mode: {mode}. Valid modes are: distinct"
        )


@task(nout=2)
def get_new_addresses(  # pylint: disable=too-many-arguments, too-many-locals
    source_dataset_id: str,
    source_table_id: str,
    source_table_address_column: str,
    destination_dataset_id: str,
    destination_table_id: str,
    georef_mode: str,
    current_flow_labels: List[str],
) -> Tuple[pd.DataFrame, bool]:
    """
    Get new addresses from source table
    """

    new_addresses = pd.DataFrame(columns=["address"])
    exists_new_addresses = False

    source_table_ref = f"{source_dataset_id}.{source_table_id}"
    destination_table_ref = f"{destination_dataset_id}.{destination_table_id}"
    billing_project_id = current_flow_labels[0]

    if georef_mode == "distinct":
        query_source = f"""
        SELECT DISTINCT
            {source_table_address_column}
        FROM
            `{source_table_ref}`
        """

        query_destination = f"""
        SELECT DISTINCT
            address
        FROM
            `{destination_table_ref}`
        """

        source_addresses = bd.read_sql(
            query_source, billing_project_id=billing_project_id, from_file=True
        )
        source_addresses.columns = ["address"]
        try:
            destination_addresses = bd.read_sql(
                query_destination, billing_project_id=billing_project_id, from_file=True
            )
            destination_addresses.columns = ["address"]
        except Exception:  # pylint: disable=broad-except
            destination_addresses = pd.DataFrame(columns=["address"])

        # pylint: disable=invalid-unary-operand-type
        new_addresses = source_addresses[
            ~source_addresses.isin(destination_addresses)
        ].dropna()
        exists_new_addresses = not new_addresses.empty

    return new_addresses, exists_new_addresses


@task
def georeference_dataframe(
    new_addresses: pd.DataFrame, log_divider: int = 60
) -> pd.DataFrame:
    """
    Georeference all addresses in a dataframe
    """
    start_time = time()

    all_addresses = new_addresses["address"].tolist()
    all_addresses = [f"{address}, Rio de Janeiro" for address in all_addresses]

    geolocator = Nominatim(user_agent="prefeitura-rio")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

    log(f"There are {len(all_addresses)} addresses to georeference")

    locations: List[Location] = []
    for i, address in enumerate(all_addresses):
        if i % log_divider == 0:
            log(f"Georeferencing address {i} of {len(all_addresses)}...")
        location = geocode(address)
        locations.append(location)

    geolocated_addresses = [
        {
            "latitude": location.latitude,
            "longitude": location.longitude,
        }
        if location is not None
        else {"latitude": None, "longitude": None}
        for location in locations
    ]

    output = pd.DataFrame(geolocated_addresses)
    output["address"] = new_addresses["address"]
    output[["latitude", "longitude"]] = output.apply(
        lambda x: check_if_belongs_to_rio(x.latitude, x.longitude),
        axis=1,
        result_type="expand",
    )

    log(f"--- {(time() - start_time)} seconds ---")

    return output


@task
def dataframe_to_csv(dataframe: pd.DataFrame, filename: str = "data.csv") -> None:
    """
    Save dataframe to csv
    """
    filename = filename if filename.endswith(".csv") else f"{filename}.csv"
    temp_filename = Path(f"/tmp/{uuid4()}/{filename}")
    temp_filename.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_csv(temp_filename, index=False)
    return str(temp_filename.parent)
