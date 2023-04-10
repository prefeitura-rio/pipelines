# -*- coding: utf-8 -*-
"""
Tasks for projeto_subsidio_sppo
"""

from typing import List
import pandas as pd
from prefect import task

from pipelines.utils.tasks import log, get_now_date
