"""degreed Authentication."""
import requests

from singer_sdk.helpers._util import utc_now
from typing import List, AnyStr
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


# The SingletonMeta metaclass makes your streams reuse the same authenticator instance.
# If this behaviour interferes with your use-case, you can remove the metaclass.
class DegreedAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Degreed."""


    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the degreed API."""
        # TODO: Define the request body needed for the API.
        return {
            #'resource': 'https://degreed.com/oauth/token',
            
            'grant_type': 'client_credentials',
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'scope': 'users:read completions:read pathways:read'
        }

    @classmethod
    def create_for_stream(cls, stream) -> "DegreedAuthenticator":
        return cls(
            stream=stream,
            auth_endpoint= "https://degreed.com/oauth/token",
            
       
        )

    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.
        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        oauth_request_body = self.oauth_request_body
        token_response = requests.post(
            self.auth_endpoint,
            headers={
                "Content-Type": "application/x-www-form-urlencoded"
            },
            data=oauth_request_body
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.json()}'. {ex}"
                f"{token_response.request.body}"
            )
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json["expires_in"]
        self.last_refreshed = request_time
            
        
