"""REST client handling, including PowerBIStream base class."""

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import requests
from memoization import cached
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_powerbi.auth import PowerBIAuthenticator


class PowerBIStream(RESTStream):
    """PowerBI stream class."""

    url_base = "https://api.powerbi.com/v1.0/myorg"

    records_jsonpath = "$.value[*]"  
    next_page_token_jsonpath = "$.next_page"  
    _page_size = 1000
    offset = 0

    @property
    @cached
    def authenticator(self) -> PowerBIAuthenticator:
        """Return a new authenticator object."""
        return PowerBIAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        next_page_token = None
        if self.name == "dataset_data":
            self.offset = self.offset + self._page_size
            all_matches = extract_jsonpath(self.records_jsonpath, response.json())
            first_match = next(iter(all_matches), None)
            if "rows" in first_match:
                if len(first_match["rows"]) == 0:
                    return None
            else:
                return None
            return self.offset

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        return params
