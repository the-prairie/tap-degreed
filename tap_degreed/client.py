"""REST client handling, including degreedStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from urllib.parse import urljoin, urlparse, parse_qs

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

from tap_degreed.auth import DegreedAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class DegreedStream(RESTStream):
    """Degreed stream class."""

    url_base = "https://api.degreed.com/api/v2"

    # OR use a dynamic url_base:
    # @property
    # def url_base(self) -> str:
    #     """Return the API URL root, configurable via tap settings."""
    #     return self.config["api_url"]

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.
    next_page_token_jsonpath = "$.links.next"  # Or override `get_next_page_token`.

    @property
    @cached
    def authenticator(self) -> DegreedAuthenticator:
        """Return a new authenticator object."""
        return DegreedAuthenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        if not self.authenticator.is_token_valid():
            self.logger.info("Fetching valid token...")
            self.authenticator.update_access_token()
        headers = {'Accept': 'application/json'}
        headers["Authorization"] = "Bearer {token}".format(token=self.authenticator.access_token)
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                self.next_page_token_jsonpath, response.json()
            )
            first_match = next(iter(all_matches), None)
            if first_match:
                parsed = urlparse(first_match)
                qs = parse_qs(parsed.query)
                next_page_token = qs['next'][0]
            else:
                next_page_token = None
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["next"] = next_page_token
        params["limit"] = 1000
            
        return params


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result rows."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

