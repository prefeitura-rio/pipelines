# -*- coding: utf-8 -*-
"""
Helper functions for generating a data catalog from BigQuery.
"""
from typing import Any, Dict, List, Union

from arcgis import GIS
from arcgis.gis import ContentManager, Item
from google.cloud import bigquery
from gspread.worksheet import Worksheet
import jinja2
import requests

from pipelines.rj_escritorio.data_catalog.constants import (
    constants as data_catalog_constants,
)
from pipelines.utils.utils import (
    get_credentials_from_env,
    get_vault_secret,
    list_blobs_with_prefix,
    send_discord_message,
)


class GisItemNotFound(Exception):
    """
    Raised when an item is not found.
    """


def build_html_from_metadata(
    html_template: jinja2.Template,
    metadata: Dict[str, Any],
) -> str:
    """
    Builds HTML from the table metadata
    """
    return html_template.render(
        **metadata,
    )


def build_items_data_from_metadata_json(metadata: List[Dict[str, Any]]) -> List[dict]:
    """
    Builds item data from a schema.yml file.
    Returns:
        A dictionary containing the item data. The format is:
        {
            item_properties={
                "type": "Document Link",
                "typeKeywords": "Data, Document",
                "description": "some description here", # We need to parse from Markdown to HTML
                "title": "some title here",
                "url": "https://some.url.here",
                "tags": "tag1, tag2, tag3",
                ...
            },
        }
    """
    items_data = []
    categories = []
    project_ids = []
    dataset_ids = []
    table_ids = []
    html_template = get_description_html_template()
    for table in metadata:
        project_id = table["project_id"]
        dataset_id = table["dataset_id"]
        table_id = table["table_id"]
        item_data = {
            "item_properties": {
                "type": "Document Link",
                "typeKeywords": "Data, Document",
                "description": build_html_from_metadata(
                    html_template=html_template,
                    metadata=table,
                ),
                "snippet": table["metadata"]["short_description"],
                "title": table["metadata"]["title"],
                "url": table["url"],
                "tags": ",".join(
                    get_default_tags(project_id, dataset_id, table_id)
                    + table["metadata"]["tags"]
                ),
                "licenseInfo": get_license(),
                "accessInformation": table["metadata"]["data_owner"],
            },
        }
        items_data.append(item_data)
        categories.append(table["metadata"]["categories"])
        project_ids.append(project_id)
        dataset_ids.append(dataset_id)
        table_ids.append(table_id)
    return items_data, categories, project_ids, dataset_ids, table_ids


def create_or_update_item(
    project_id: str, dataset_id: str, table_id: str, data: dict
) -> Item:
    """
    Creates or updates an item.
    """
    gis = get_gis_client()
    tags = get_default_tags(project_id, dataset_id, table_id)
    tags_query = ""
    for tag in tags:
        if tags_query == "":
            tags_query = f"tags:{tag}"
        else:
            tags_query += f" AND tags:{tag}"
    try:  # If item already exists, update it.
        item = get_item(
            search_query=tags_query,
            gis=gis,
        )
        item.update(**data)
        return item
    except GisItemNotFound:  # Else, create it.
        content_manager: ContentManager = gis.content
        item = content_manager.add(**data)  # pylint: disable=no-member
        return item


def fetch_api_metadata(
    project_id: str, dataset_id: str, table_id: str
) -> Dict[str, Any]:
    """
    Fetches table metadata from the metadata API.

    Args:
        project_id: Project ID.
        dataset_id: Dataset ID.
        table_id: Table ID.

    Returns:
        A dictionary in the following format:
        {
            "title": "Title",
            "short_description": "Short description",
            "long_description": "Long description",
            "update_frequency": "Update frequency",
            "temporal_coverage": "Temporal coverage",
            "data_owner": "Data owner",
            "publisher_name": "Publisher name",
            "publisher_email": "Publisher email",
            "tags": ["Tag1", "Tag2"],
            "categories": ["Category1", "Category2"],
            "columns": [
                {
                    "name": "column_name",
                    "description": "Column description",
                }
            ]
        }
    """
    metadata = {
        "title": None,
        "short_description": None,
        "long_description": None,
        "update_frequency": None,
        "temporal_coverage": None,
        "data_owner": None,
        "publisher_name": None,
        "publisher_email": None,
        "tags": [],
        "categories": [],
        "columns": [],
    }
    base_url = "https://meta.dados.rio/api"
    dataset_metadata = fetch_single_result(
        f"{base_url}/datasets/?project={project_id}&name={dataset_id}"
    )
    if dataset_metadata is None:
        notify_missing_metadata(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=None,
        )
        raise Exception(
            f"Dataset metadata not found for project {project_id} and dataset {dataset_id}"
        )
    title_prefix = dataset_metadata["title_prefix"]
    table_metadata = fetch_single_result(
        f"{base_url}/tables/?project={project_id}&dataset={dataset_id}&name={table_id}"
    )
    if table_metadata is None:
        notify_missing_metadata(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
        )
        raise Exception(
            f"Table metadata not found for project {project_id}, dataset {dataset_id} and table {table_id}"  # noqa, pylint: disable=line-too-long
        )
    metadata["title"] = f"{title_prefix}: {table_metadata['title']}"
    metadata["short_description"] = table_metadata["short_description"]
    metadata["long_description"] = table_metadata["long_description"]
    metadata["update_frequency"] = table_metadata["update_frequency"]
    metadata["temporal_coverage"] = table_metadata["temporal_coverage"]
    metadata["data_owner"] = table_metadata["data_owner"]
    metadata["publisher_name"] = table_metadata["publisher_name"]
    metadata["publisher_email"] = table_metadata["publisher_email"]
    metadata["tags"] = table_metadata["tags"]
    metadata["categories"] = table_metadata["categories"]
    metadata["columns"] = []
    for column in table_metadata["columns"]:
        metadata["columns"].append(
            {
                "name": column["name"],
                "description": column["description"],
            }
        )
    return metadata


def fetch_single_result(url: str) -> Union[Dict[str, Any], None]:
    """
    Fetches a DRF API and returns None if there's more than one result or no result at all.
    """
    response = requests.get(url)
    response.raise_for_status()
    response_json = response.json()
    if response_json["count"] == 1:
        return response_json["results"][0]
    return None


def get_all_items() -> List[Item]:
    """
    Lists all datalake items on ArcGIS Online.
    """
    search_query = "tags:datalake"
    gis = get_gis_client()
    content_manager: ContentManager = gis.content
    items = content_manager.search(search_query)
    return items


def get_bigquery_client(mode: str = "prod") -> bigquery.Client:
    """
    Get BigQuery client.

    Returns:
        BigQuery client.
    """
    credentials = get_credentials_from_env(mode=mode)
    client = bigquery.Client(credentials=credentials)
    return client


def get_default_tags(project_id: str, dataset_id: str, table_id: str) -> List[str]:
    """
    Returns the default tags.
    """
    return ["datario", "escritorio_de_dados", "datalake"] + [
        project_id,
        dataset_id,
        table_id,
    ]


def get_description_html_template() -> jinja2.Template:
    """
    Downloads the Jinja template from GCS and then returns it.
    """
    blobs = list_blobs_with_prefix(
        bucket_name=data_catalog_constants.GCS_BUCKET_NAME.value,
        prefix=data_catalog_constants.DESCRIPTION_HTML_TEMPLATE_PATH.value,
    )
    blob = blobs[0]
    template = blob.download_as_string().decode("utf-8")
    return jinja2.Template(template)


def get_directory(
    project_id: str, dataset_id: str, table_id: str, duplicates_list: List[str] = None
) -> str:
    """
    Returns the directory where the item will be stored.
    """
    duplicates_list = duplicates_list or data_catalog_constants.DONT_PUBLISH.value
    full_name = f"{project_id}.{dataset_id}.{table_id}"
    if full_name in duplicates_list:
        return "duplicates"
    return "public"


def get_gis_client() -> GIS:
    """
    Returns a GIS client.
    """
    gis_credentials = get_vault_secret(
        data_catalog_constants.ARCGIS_CREDENTIALS_SECRET_PATH.value
    )["data"]
    url = gis_credentials["url"]
    username = gis_credentials["username"]
    password = gis_credentials["password"]
    return GIS(url, username, password)


def get_item(*, item_id: str = None, search_query: str = None, gis: GIS = None) -> Item:
    """
    Returns an item.
    """
    # Ensures that either item_id or search is provided.
    if not search_query and not item_id:
        raise ValueError("You must provide either an item_id or a search query.")
    # Get GIS client and the content manager.
    gis = gis or get_gis_client()
    content_manager: ContentManager = gis.content
    # If item_id is provided, get and return the item.
    if item_id:
        item = content_manager.get(item_id)
        if not item:
            raise GisItemNotFound(f"Item with id {item_id} not found.")
        return item
    # Else, search for the item.
    items = content_manager.search(search_query)
    if len(items) == 0:
        raise GisItemNotFound(f"No items found for search query: {search_query}")
    return items[0]


def get_license():
    """
    Returns the license (can be HTML formatted).
    """
    cc_license = "by-nd/3.0"
    cc_license_name = "Attribution-NoDerivatives 3.0"
    license_url = f"https://creativecommons.org/licenses/{cc_license}/deed.pt_BR"
    return f"""
    <a href={license_url}>
        <img    alt="Creative Commons License"
                src="https://i.creativecommons.org/l/{cc_license}/88x31.png"
                style="border-width:0"
        />
    </a>
    <br />
    This work is licensed under a
    <a rel="license" href={license_url}>Creative Commons {cc_license_name} Unported License</a>.
    """


def notify_missing_metadata(project_id: str, dataset_id: str, table_id: str) -> None:
    """
    Notifies in Discord that the metadata is missing.
    """
    if table_id is None:
        message = f"Metadata for dataset `{project_id}.{dataset_id}` is missing."
    else:
        message = (
            f"Metadata for table `{project_id}.{dataset_id}.{table_id}` is missing."
        )
    webhook_url = get_vault_secret(
        data_catalog_constants.DISCORD_WEBHOOK_SECRET_PATH.value
    )["data"]["url"]
    send_discord_message(message=message, webhook_url=webhook_url)


def write_data_to_gsheets(
    worksheet: Worksheet, data: List[List[Any]], start_cell: str = "A1"
):
    """
    Write data to a Google Sheets worksheet.

    Args:
        worksheet: Google Sheets worksheet.
        data: List of lists of data.
        start_cell: Cell to start writing data.
    """
    try:
        start_letter = start_cell[0]
        start_row = int(start_cell[1:])
    except ValueError as exc:
        raise ValueError("Invalid start_cell. Please use a cell like A1.") from exc
    cols_len = len(data[0])
    rows_len = len(data)
    end_letter = chr(ord(start_letter) + cols_len - 1)
    if end_letter not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        raise ValueError("Too many columns. Please refactor this code.")
    end_row = start_row + rows_len - 1
    range_name = f"{start_letter}{start_row}:{end_letter}{end_row}"
    worksheet.update(range_name, data)
