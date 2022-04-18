"""
Helper tasks that could fit any pipeline.
"""

from prefect import task

from pipelines.utils import (
    get_username_and_password_from_secret,
    log
)

##################
#
# Hashicorp Vault
#
##################


@task(checkpoint=False)
def get_user_and_password(secret_path: str):
    """
    Returns the user and password for the given secret path.
    """
    log(f"Getting user and password for secret path: {secret_path}")
    return get_username_and_password_from_secret(secret_path)
