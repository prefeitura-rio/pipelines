from prefect import task
from pipelines.utils.utils import log

@task
def log_all(value) -> None:
    log(value)