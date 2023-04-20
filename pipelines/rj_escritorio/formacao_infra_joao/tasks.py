# -*- coding: utf-8 -*-
from prefect import task
from pipelines.utils.utils import log

import requests as rq
import pandas as pd
import matplotlib.pyplot as plt
import json
from pandas.io.json import json_normalize

from datetime import datetime
import os.path
import pyarrow.parquet as pq
import pyarrow as pa


@task
def hello_name(name: str) -> None:
    log(f"Hello {name}!")
