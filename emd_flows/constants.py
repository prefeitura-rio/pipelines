"""
Constants for all flows
"""
from enum import Enum


class constants(Enum):  # pylint: disable=c0103
    """
    Constants used in the EMD flows.
    """
    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"

    ######################################
    # Other constants
    ######################################
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
