# -*- coding: utf-8 -*-
"""
Tasks for the flooding notification pipeline.
"""
from typing import Any, Dict, List, Tuple, Union
from uuid import uuid4

from prefect import task
import requests

from pipelines.rj_escritorio.notify_flooding.utils import (
    get_circle,
    send_email,
)
from pipelines.utils.utils import (
    get_redis_client,
    get_vault_secret,
    log,
)


@task
def parse_comma_separated_string_to_list(
    input_text: str,
    output_type: type = int,
) -> List[Any]:
    """
    Parse a comma separated string to a list.

    Args:
        input: Input string.
        output_type: Type of the output list.

    Returns:
        List of the input string elements.
    """
    if input_text == "":
        return []
    return [output_type(element) for element in input_text.split(",")]


@task
def get_open_occurrences(api_url: str) -> List[Dict[str, Union[str, int, float]]]:
    """
    Get open occurrences from the API.

    Args:
        api_url: URL to the COR-Comando API (open occurences endpoint)

    Returns:
        List of open occurrences.
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
    except Exception as exc:
        raise Exception(f"Error getting open occurrences from API: {exc}") from exc
    try:
        data = response.json()
    except Exception as exc:
        raise Exception(f"Error parsing response from API: {exc}") from exc
    try:
        occurences = data["eventos"]
    except KeyError as exc:
        raise Exception(f"Error parsing response from API: {exc}") from exc
    return occurences


@task
def filter_flooding_occurences(
    open_occurrences: List[Dict[str, Union[str, int, float]]],
    flooding_pop_id: Union[int, List[int]],
) -> List[Dict[str, Union[str, int, float]]]:
    """
    Filter flooding occurrences from the API response.

    Args:
        open_occurrences: List of open occurrences from the API.
        flooding_pop_id: ID or list of IDs of the flooding POPs.

    Returns:
        List of flooding occurrences.
    """
    if isinstance(flooding_pop_id, int):
        flooding_pop_id = [flooding_pop_id]
    flooding_occurrences = [
        occurrence
        for occurrence in open_occurrences
        if occurrence["pop_id"] in flooding_pop_id
    ]
    return flooding_occurrences


@task
def get_cached_flooding_occurences(
    redis_key: str,
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: str = None,
) -> List[Dict[str, Union[str, int, float]]]:
    """
    Get flooding occurrences from Redis.

    Args:
        redis_key: Key to the flooding occurrences in Redis.
        host: Redis host.
        port: Redis port.
        db: Redis database.
        password: Redis password.

    Returns:
        List of flooding occurrences.
    """
    redis_client = get_redis_client(host=host, port=port, db=db, password=password)
    flooding_occurrences = redis_client.get(redis_key)
    if flooding_occurrences is None:
        flooding_occurrences = []
    return flooding_occurrences


@task(nout=3)
def compare_flooding_occurences(
    from_api: List[Dict[str, Union[str, int, float]]],
    from_cache: List[Dict[str, Union[str, int, float]]],
) -> Tuple[
    List[Dict[str, Union[str, int, float]]],
    List[Dict[str, Union[str, int, float]]],
]:
    """
    Compare flooding occurrences from the API with the ones in the cache.

    Args:
        from_api: List of flooding occurrences from the API.
        from_cache: List of flooding occurrences from the cache.

    Returns:
        Tuple with the new flooding occurrences, the closed flooding occurrences and the
        current flooding occurrences.
    """
    ids_from_api = [occurrence["id"] for occurrence in from_api]
    log(f"IDs from API: {ids_from_api}")
    ids_from_cache = [occurrence["id"] for occurrence in from_cache]
    log(f"IDs from cache: {ids_from_cache}")
    new_occurrences = [
        occurrence for occurrence in from_api if occurrence["id"] not in ids_from_cache
    ]
    log(f"New occurrences: {[occurrence['id'] for occurrence in new_occurrences]}")
    closed_occurrences = [
        occurrence for occurrence in from_cache if occurrence["id"] not in ids_from_api
    ]
    log(
        f"Closed occurrences: {[occurrence['id'] for occurrence in closed_occurrences]}"
    )
    current_occurences = [
        occurence for occurence in from_cache if occurence["id"] in ids_from_api
    ]
    log(
        f"Current occurrences: {[occurrence['id'] for occurrence in current_occurences]}"
    )
    current_occurences += new_occurrences
    return new_occurrences, closed_occurrences, current_occurences


@task
def update_flooding_occurences_cache(  # pylint: disable=R0913
    flooding_occurrences: List[Dict[str, Union[str, int, float]]],
    redis_key: str,
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,  # pylint: disable=C0103
    password: str = None,
):
    """
    Update the flooding occurrences cache.

    Args:
        flooding_occurrences: List of flooding occurrences.
        redis_key: Key to the flooding occurrences in Redis.
        host: Redis host.
        port: Redis port.
        db: Redis database.
        password: Redis password.
    """
    redis_client = get_redis_client(host=host, port=port, db=db, password=password)
    redis_client.set(redis_key, flooding_occurrences)


@task
def send_email_for_flooding_occurence(
    occurence: Dict[str, Union[str, int, float]],
    mode: str,
    to_email: Union[str, List[str]],
    email_configuration_secret_path: str,
    radius: int = 10,
):
    """
    Send an email for a flooding occurrence.

    Args:
        occurence: Flooding occurrence.
        mode: Must be "new" or "closed".
        to_email: Email (or list of emails) to send the email to.
        email_configuration_secret_path: Path to the from email in Vault. This provides username,
            password and SMTP server.
    """
    try:
        radius = int(radius)
    except ValueError as exc:
        raise ValueError(f"Invalid radius: {radius}") from exc
    if mode not in ["new", "closed"]:
        raise ValueError(f"Invalid mode: {mode}")
    secret = get_vault_secret(email_configuration_secret_path)["data"]
    if mode == "new":
        subject = f"NEW FLOOD OCCURENCE - ID {occurence['id']}"
        body = subject
        circle_fname = f"{uuid4()}.kml"
        get_circle(
            latitude=occurence["latitude"],
            longitude=occurence["longitude"],
            radius=radius,
            fname=circle_fname,
        )
        attachment = circle_fname
    else:
        subject = f"CLOSED FLOOD OCCURENCE - ID {occurence['id']}"
        body = subject
        circle_fname = f"{uuid4()}.kml"
        get_circle(
            latitude=occurence["latitude"],
            longitude=occurence["longitude"],
            radius=radius,
            fname=circle_fname,
        )
        attachment = circle_fname
    send_email(
        from_address=secret["smtp_username"],
        to_address=to_email,
        subject=subject,
        body=body,
        smtp_server=secret["smtp_server"],
        smtp_port=int(secret["smtp_port"]),
        smtp_username=secret["smtp_username"],
        smtp_password=secret["smtp_password"],
        tls=True,
        attachment=attachment,
    )
