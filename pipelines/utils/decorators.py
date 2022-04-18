# -*- coding: utf-8 -*-
"""
Custom decorators for pipelines.
"""

from functools import partial

from prefect import Flow as PrefectFlow

from pipelines.constants import constants
from pipelines.utils.utils import notify_discord_on_failure


Flow = partial(
    PrefectFlow,
    on_failure=partial(
        notify_discord_on_failure,
        secret_path=constants.EMD_DISCORD_WEBHOOK_SECRET_PATH.value,
    ),
)
