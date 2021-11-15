"""degreed tap class."""

import pendulum
from typing import List


from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    DateTimeType,
    PropertiesList,
    Property,
    StringType
)

# TODO: Import your custom stream types here:
from tap_degreed.streams import (
    UsersStream,
    CompletionsStream,
    # AccomplishmentsStream,
    # PathwaysStream
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
    CompletionsStream,
    # AccomplishmentsStream,
    # PathwaysStream
]


class TapDegreed(Tap):
    """Degreed tap class."""

    name = "tap-degreed"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = PropertiesList(
        Property(
            "client_id", 
            StringType, 
            required=True, 
            description="The token to authenticate against the API service"
        ),
        Property(
            "client_secret", 
            StringType, 
            required=True, 
            description="The token to authenticate against the API service"
        ),
        Property(
            "api_url",
            StringType,
            default="https://degreed.com/oauth/token",
            description="The url for the API service"
        ),
        Property(
            "start_date",
            DateTimeType,
            default=pendulum.today(tz='America/Edmonton').add(days=-2).strftime("%Y-%m-%d"),
            description="The earliest record date to sync. Expected format YYYY-mm-dd."
        ),
        Property(
            "end_date",
            DateTimeType,
            default=pendulum.today(tz='America/Edmonton').add(days=-1).strftime("%Y-%m-%d"),
            description="The earliest record date to sync. Expected format YYYY-mm-dd."
        ),
        
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream(tap=self) for stream in STREAM_TYPES]



# CLI Execution:

cli = TapDegreed.cli
