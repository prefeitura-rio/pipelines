from pipelines.utils.utils import run_local
from pipelines.rj_escritorio.template_pipeline.flows import flow

run_local(flow, parameters = {"param": "val"})