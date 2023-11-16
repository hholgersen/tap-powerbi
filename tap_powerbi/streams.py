"""Stream type classes for tap-powerbi."""


from typing import Any, Optional

from singer_sdk import typing as th

from tap_powerbi.client import PowerBIStream


class ReportsStream(PowerBIStream):
    """Define custom stream."""

    name = "reports"
    path = "/reports"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("embedUrl", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "dataset_id": record["datasetId"],
        }


class DataSetsStream(PowerBIStream):
    """Define custom stream."""

    name = "datasets"
    path = "/datasets/{dataset_id}"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$[*]"
    parent_stream_type = ReportsStream
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("webUrl", th.StringType),
        th.Property("configuredBy", th.StringType),
        th.Property("isRefreshable", th.BooleanType),
        th.Property("isEffectiveIdentityRequired", th.BooleanType),
        th.Property("isEffectiveIdentityRolesRequired", th.BooleanType),
        th.Property("isOnPremGatewayRequired", th.BooleanType),
        th.Property("targetStorageMode", th.StringType),
        th.Property("createReportEmbedURL", th.StringType),
        th.Property("qnaEmbedURL", th.StringType),
        th.Property("upstreamDatasets", th.CustomType({"type": ["array", "string"]})),
        th.Property("users", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "dataset_id": record["id"],
            "dataset_name": record["name"],
        }


class DataSetDataStream(PowerBIStream):
    """Define custom stream."""

    name = "dataset_data"
    path = "/datasets/{dataset_id}/executeQueries"
    rest_method = "POST"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.results.[*].tables.[*]"
    parent_stream_type = DataSetsStream

    schema = th.PropertiesList(
        th.Property("datasetId", th.StringType),
        th.Property("dataset_name", th.StringType),
        th.Property("rows", th.CustomType({"type": ["array", "string"]})),
    ).to_dict()

    def prepare_request_payload(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Optional[dict]:

        query = {
            "queries": [
                {
                    "query": f"EVALUATE TopnSkip({self._page_size},{self.offset},'{context.get('dataset_name')}')"
                }
            ],
            "serializerSettings": {"includeNulls": True},
        }
        return query

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        row["datasetId"] = context.get("dataset_id")
        row["dataset_name"] = context.get("dataset_name")
        return row
