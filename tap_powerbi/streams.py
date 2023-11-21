"""Stream type classes for tap-powerbi."""


from typing import Any, Optional

from singer_sdk import typing as th

from tap_powerbi.client import PowerBIStream
from http import HTTPStatus
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.pagination import BaseHATEOASPaginator
import requests
from time import sleep
import datetime
from urllib.parse import urlencode
import typing as t

class MyHATEOASPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        return response.json().get("continuationUri")


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


class DataSetsStream(PowerBIStream):
    """Define custom stream."""

    name = "datasets"
    path = "/datasets/"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$value.[*]"
    # parent_stream_type = ReportsStream
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

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.
        """

        if response.status_code in [404, 400]:
            return
        
        if (
            response.status_code in self.extra_retry_statuses
            or response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        if (
            HTTPStatus.BAD_REQUEST
            <= response.status_code
            < HTTPStatus.INTERNAL_SERVER_ERROR
        ):
            msg = self.response_error_message(response)
            raise FatalAPIError(msg)


    # def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
    #     """Return a context dictionary for child streams."""
    #     return {
    #         "dataset_id": record.get("id"),
    #         "dataset_name": record.get("name"),
    #     }


# class DataSetDataStream(PowerBIStream):
#     """Define custom stream."""

#     name = "dataset_data"
#     path = "/datasets/{dataset_id}/executeQueries"
#     rest_method = "POST"
#     primary_keys = ["id"]
#     replication_key = None
#     records_jsonpath = "$.results.[*].tables.[*]"
#     parent_stream_type = DataSetsStream


#     def validate_response(self, response: requests.Response) -> None:
#         """Validate HTTP response.
#         """

#         if response.status_code in [404, 400]:
#             return

#         if (
#             response.status_code in self.extra_retry_statuses
#             or response.status_code >= HTTPStatus.INTERNAL_SERVER_ERROR
#         ):
#             msg = self.response_error_message(response)
#             raise RetriableAPIError(msg, response)

#         if (
#             HTTPStatus.BAD_REQUEST
#             <= response.status_code
#             < HTTPStatus.INTERNAL_SERVER_ERROR
#         ):
#             msg = self.response_error_message(response)
#             raise FatalAPIError(msg)


#     schema = th.PropertiesList(
#         th.Property("datasetId", th.StringType),
#         th.Property("dataset_name", th.StringType),
#         th.Property("rows", th.CustomType({"type": ["array", "string"]})),
#     ).to_dict()

#     def prepare_request_payload(
#         self, context: Optional[dict], next_page_token: Optional[Any]
#     ) -> Optional[dict]:

#         query = {
#             "queries": [
#                 {
#                     "query": f"EVALUATE TopnSkip({self._page_size},{self.offset},'{context.get('dataset_name')}')"
#                 }
#             ],
#             "serializerSettings": {"includeNulls": True},
#         }
#         return query

#     def post_process(self, row: dict, context: Optional[dict]) -> dict:
#         row["datasetId"] = context.get("dataset_id")
#         row["dataset_name"] = context.get("dataset_name")
#         return row



class WorkspaceInfoStream(PowerBIStream):
    """Define custom stream."""

    name = "workspace_info"
    path = "/workspaces/modified"
    rest_method = "GET"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$[*]"
    # TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY



    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("description", th.StringType),
        th.Property("type", th.StringType),
        th.Property("state", th.StringType),
        th.Property("isOnDedicatedCapacity", th.BooleanType),
        th.Property("capacityId", th.StringType),
        th.Property("defaultDatasetStorageFormat", th.StringType),
        th.Property("reports", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("dashboards", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("datasets", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("dataflows", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("datamarts", th.ArrayType(wrapped_type=th.ObjectType())),
    ).to_dict()


    def process_workspace_batch(self, workspace_ids):
        """
        Request workspace info for a batch of workspaces.
        """

        workspace_info_req = 'https://api.powerbi.com/v1.0/myorg/admin/workspaces/getInfo' \
                            '?lineage=True' \
                            '&datasourceDetails=True' \
                            '&datasetSchema=True' \
                            '&datasetExpressions=True' 

        workspace_info_req_body = {
        "workspaces": workspace_ids
        }

        self.logger.info(f"Body: {workspace_info_req_body}")

        headers = {
            'Authorization': f"Bearer {self.authenticator.access_token}",
            'Content-Type': 'application/json'
        }

        info_query = requests.post(
            workspace_info_req, 
            headers=headers, 
            json=workspace_info_req_body
            )
        
        info_query.raise_for_status()
        scan_id = info_query.json()["id"]
        self.logger.info(f"Scan ID: {scan_id}")
        status_url = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanStatus/{scan_id}"
        finished = False
        sleep(3)
        
        while not finished:
            
            scan = requests.get(status_url, headers=headers)
            scan.raise_for_status()
            scan_status = scan.json()

            if scan_status["status"] == "Succeeded":
                finished = True
                self.logger.info(f"Scan status: {scan_status['status']}, returning results...")
            else:
                self.logger.info(f"Scan status: {scan_status['status']}, waiting 10 seconds...")

            sleep(10)

        final_result_r = f"https://api.powerbi.com/v1.0/myorg/admin/workspaces/scanResult/{scan_id}"

        result = requests.get(final_result_r, headers=headers)
        result.raise_for_status()

        rj = result.json()

        for row in rj["workspaces"]:
            yield row


    def parse_response(self, response: requests.Response):
        """Parse the response and return an iterator of result records.
        """

        workspace_ids = [w['id'] for w in response.json()]
        workspace_chunks = [workspace_ids[i:i + 100] for i in range(0, len(workspace_ids), 100)]

        for chunk in workspace_chunks:
            yield from self.process_workspace_batch(chunk)



class ActivityEventsStream(PowerBIStream):
    """Define custom stream."""

    name = "activity_events"
    path = "/activityevents"
    rest_method = "GET"
    primary_keys = ["id"]
    replication_key = "CreationTime"
    records_jsonpath = "$.activityEventEntities[*]"  
    # TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY


    def get_new_paginator(self):
        return MyHATEOASPaginator()


    def get_url_params(self, context, next_page_token):

        if next_page_token is not None:
            return urlencode({"continuationToken": next_page_token}, safe=':-. ()%#')
        
        start_date = self.get_starting_timestamp(context) or (datetime.datetime.now() - datetime.timedelta(hours=8)).isoformat(timespec='seconds')
        end_date = datetime.datetime.utcnow().isoformat(timespec='seconds')

        param_string = urlencode({"startDateTime": f"'{start_date}'", "endDateTime": f"'{end_date}'"}, safe="':-. ()")

        return param_string


    def build_prepared_request(
        self,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> requests.PreparedRequest:
        """Build a generic but authenticated request.

        Uses the authenticator instance to mutate the request with authentication.

        Args:
            *args: Arguments to pass to `requests.Request`_.
            **kwargs: Keyword arguments to pass to `requests.Request`_.

        Returns:
            A `requests.PreparedRequest`_ object.

        .. _requests.PreparedRequest:
            https://requests.readthedocs.io/en/latest/api/#requests.PreparedRequest
        .. _requests.Request:
            https://requests.readthedocs.io/en/latest/api/#requests.Request
        """
        request = requests.Request(*args, **kwargs)
        #self.requests_session.auth = self.authenticator
        prepped_request = self.requests_session.prepare_request(request)
        #request.auth = self.authenticator
        prepped_request2 = request.prepare()
        prepped_request2.prepare_auth(self.authenticator)

        prepped_request.headers.update(prepped_request2.headers)
        
        return prepped_request


    schema = th.PropertiesList(
        th.Property("Id", th.StringType),
        th.Property("RecordType", th.IntegerType),
        th.Property("CreationTime", th.StringType),
        th.Property("Operation", th.StringType),
        th.Property("OrganizationId", th.StringType),
        th.Property("UserType", th.IntegerType),
        th.Property("UserKey", th.StringType),
        th.Property("Workload", th.StringType),
        th.Property("UserId", th.StringType),
        th.Property("ClientIP", th.StringType),
        th.Property("UserAgent", th.StringType),
        th.Property("Activity", th.StringType),
        th.Property("ItemName", th.StringType),
        th.Property("WorkSpaceName", th.StringType),
        th.Property("DatasetName", th.StringType),
        th.Property("ReportName", th.StringType),
        th.Property("CapacityId", th.StringType),
        th.Property("CapacityName", th.StringType),
        th.Property("WorkspaceId", th.StringType),
        th.Property("AppName", th.StringType),
        th.Property("ObjectId", th.StringType),
        th.Property("DatasetId", th.StringType),
        th.Property("ReportId", th.StringType),
        th.Property("ArtifactId", th.StringType),
        th.Property("ArtifactName", th.StringType),
        th.Property("IsSuccess", th.BooleanType),
        th.Property("ReportType", th.StringType),
        th.Property("RequestId", th.StringType),
        th.Property("ActivityId", th.StringType),
        th.Property("AppReportId", th.StringType),
        th.Property("DistributionMethod", th.StringType),
        th.Property("ConsumptionMethod", th.StringType),
        th.Property("AppId", th.StringType),
        th.Property("ArtifactKind", th.StringType),
        th.Property("RefreshEnforcementPolicy", th.IntegerType),
        th.Property("ExportedArtifactInfo", th.ObjectType()),
        th.Property("AggregatedWorkspaceInformation", th.ObjectType()),
        th.Property("SensitivityLabelId", th.StringType),
        th.Property("DashboardName", th.StringType),
        th.Property("DashboardId", th.StringType),
        th.Property("Datasets", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("ModelsSnapshots", th.ArrayType(wrapped_type=th.ObjectType())),
        th.Property("DataConnectivityMode", th.StringType),
        th.Property("RefreshType", th.StringType),
        th.Property("LastRefreshTime", th.StringType),
        th.Property("ArtifactAccessRequestInfo", th.ObjectType()),
        th.Property("ImportId", th.StringType),
        th.Property("ImportSource", th.StringType),
        th.Property("ImportType", th.StringType),
        th.Property("ImportDisplayName", th.StringType),
        th.Property("ActionSource", th.StringType),
        th.Property("LabelEventType", th.StringType),
        th.Property("ActionSourceDetail", th.StringType),
        th.Property("ArtifactType", th.StringType),
        th.Property("SensitivityLabelEventData", th.ObjectType()),
        th.Property("HasFullReportAttachment", th.BooleanType),
        th.Property("SubscriptionDetails", th.ObjectType())
    ).to_dict()

