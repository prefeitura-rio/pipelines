# -*- coding: utf-8 -*-
from google.cloud.dataplex_v1 import (
    DataScan,
    UpdateDataScanRequest,
    DataScanServiceClient,
    GetDataScanRequest,
    RunDataScanRequest,
)
from google.oauth2 import service_account
import json
import os
import base64


class Dataplex:
    def __init__(self):
        self.base_url = "https://google.com/dataplex"


class DataQuality:
    def __init__(
        self,
        data_scan_id: str,
        credentials_path: str = None,
        project: str = None,
        location: str = "us-central1",
    ):
        self.credentials_path = credentials_path
        self.credentials = self._load_credentials()
        self.location = location
        self.project = project or self.credentials._project_id
        self.id = data_scan_id
        self.client = DataScanServiceClient(credentials=self.credentials)
        self.scan = self._get_scan()
        self.spec = self.scan.data_quality_spec

    @staticmethod
    def _decode_env(env: str) -> str:
        """
        Decode environment variable
        Adapted from basedosdados.upload.base.Base
        """
        return base64.b64decode(os.getenv(env).encode("utf-8")).decode("utf-8")

    def _load_credentials(self):
        """
        Load credentials from env or filepath

        Returns:
            _type_: _description_
        """
        json_acct_info = None
        if os.getenv("BASEDOSDADOS_CREDENTIALS_PROD"):
            stream = self._decode_env("BASEDOSDADOS_CREDENTIALS_PROD")
            json_acct_info = json.loads(stream, strict=False)
        elif self.credentials_path:
            with open(self.credentials_path) as fp:
                json_acct_info = json.load(fp, strict=False)

        credentials = service_account.Credentials.from_service_account_info(
            json_acct_info
        )
        return credentials

    def _get_scan(self):
        """
        Fetches DataScan definitions

        Returns:
            google.cloud.dataplex_v1.DataScan
        """
        try:
            # Initialize request argument(s)
            request = GetDataScanRequest(
                name=f"projects/{self.project}/locations/{self.location}/dataScans/{self.id}",
                view=GetDataScanRequest.DataScanView.FULL.value,
            )
            # Make the request
            response = self.client.get_data_scan(request=request)
            return response
        except Exception:
            return None

    def run(self):
        # Initialize request argument(s)
        request = RunDataScanRequest(
            name=f"projects/{self.project}/locations/{self.location}/dataScans/{self.id}",
        )
        # Make the request
        response = self.client.run_data_scan(request=request)
        return response

    def _create_default(self):
        pass

    def _patch(self, row_filter: str, sampling_percent: float = None):
        # self.scan.data_quality_spec.row_filter = row_filter
        self.scan.data_profile_spec.sampling_percent = sampling_percent
        update_mask = [
            "dataQualitySpec.rowFilter",
            # "dataQualitySpec.samplingPercent", #TODO: patching sampling percent goes to 0.0
        ]
        request = UpdateDataScanRequest(
            data_scan=self.scan, update_mask=",".join(update_mask)
        )
        # Make the request
        operation = self.client.update_data_scan(request=request)

        response = operation.result()
        return response

    def run_parameterized(self, value: str, column: str = "data", operator: str = ">="):
        pass

    def add_rules(self, rules: dict):
        # get current scan definition
        # add new rules
        # update scan definition
        pass


class DataProfile(Dataplex):
    def __init__(self):
        pass
