"""Stream type classes for tap-degreed."""

import pendulum
from pathlib import Path
from typing import Any, Dict, Generator, Optional, Union, List, Iterable
import requests
import backoff

from singer_sdk.typing import (
    PropertiesList,
    Property,
    DateTimeType,
    NumberType,
    IntegerType,
    StringType,
    ArrayType,
    ObjectType,
    BooleanType
)


from tap_degreed.client import DegreedStream


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


# rearrange for targets like BigQuery
def rearrange_schema(schema: dict):
    props = schema.get('properties', schema)
    for k, v in props.items():
        if 'type' in v:
            if isinstance(v['type'], list):
                temp = v['type'][0]
                v['type'][0] = v['type'][1]
                v['type'][1] = temp
        if 'properties' in v:
            v['properties'] = rearrange_schema(v['properties'])
        if 'items' in v:
            v['items'] = rearrange_schema(v['items'])
    return schema

class UsersStream(DegreedStream):
    name = "users"
    path = "/users"
    primary_keys = ["id"]
    #replication_key = "$attributes.last-login-at"
    
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    schema_filepath = SCHEMAS_DIR / "users.json"
    # schema = rearrange_schema(
    # PropertiesList(
    #     Property("type", StringType),
    #     Property("id", StringType),
    #     Property("attributes", 
    #                 ObjectType(
    #                     Property("employee-id", StringType),
    #                     Property("first-name", StringType),
    #                     Property("last-name", StringType),
    #                     Property("full-name", StringType),
    #                     Property("organization-email", StringType),
    #                     Property("personal-email", StringType),
    #                     Property("profile-visibility", StringType),
    #                     Property("bio", StringType),
    #                     Property("location", StringType),
    #                     Property("profile-image-url", StringType),
    #                     Property("login-disabled", BooleanType),
    #                     Property("restricted", BooleanType),
    #                     Property("permission-role", StringType),
    #                     Property("real-time-email-notification", BooleanType),
    #                     Property("daily-digest-email", BooleanType),
    #                     Property("weekly-digest-email", BooleanType),
    #                     Property("created-at", DateTimeType),
    #                     Property("first-login-at", DateTimeType),
    #                     Property("last-login-at", DateTimeType),
    #                     Property("total-points", NumberType),
    #                     Property("daily-logins", IntegerType)
    #                 )),
    #     Property("relationships", 
    #                 ArrayType(
    #                     ObjectType(
    #                         Property("manager", ObjectType(
    #                             Property("data", ObjectType(
    #                                 Property("id", StringType),
    #                                 Property("type", StringType)
    #                             ))
    #                         ))

    #                 )))
    # ).to_dict())

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        
        return params

class CompletionsStream(DegreedStream):
    name = "completions"
    path = "/completions"
    primary_keys = ["id"]
    #replication_key = "completed-at"

    schema_filepath = SCHEMAS_DIR / "completions.json"

    
    def get_records(self, context: Optional[dict]) -> Generator[dict, None, None]:
        
        """ Get endpoint history.

        #     Raises: 
        #       ValueError: When parameter start_date is missing

        #     Yields:
        #       Generator[dict]: Yields Degreed endpoint over time range
     
        """
    # Validate start_date value
        self.logger.info('Creating stream for Degreed historical data pull')
    
        start_date_input: str = str(self.config.get("start_date"))
        end_date_input: str = str(self.config.get('end_date', pendulum.yesterday().format("YYYY-MM-DD")))
        self.logger.info(f"Start date is {start_date_input}")
        self.logger.info(f"End date is {end_date_input}")

        if not start_date_input:
            raise ValueError('The parameter start_date is required.')
        
        # Set start date and end date
        start_date  = pendulum.parse(start_date_input)
        end_date = pendulum.parse(end_date_input)

        self.logger.info(
            f"Retrieving completions from {start_date.format('Y-M-D')} to {end_date.format('Y-M-D')}"
        )

 

        # Maximum date range between start_date and end_date is 7 days
        # Split requests into weekly batches
        batches: range = (pendulum.period(start_date, end_date).range('weeks'))

        current_batch: int = 0
        # Batches contain all start_dates, the end_date is 6 days 23:59 later
        # E.g. 2021-01-01T00:00:00+0000 <--> 2021-01-07T23:59:59+0000
        for start_date_batch in batches:
            end_date_batch = (
                start_date_batch.add(days=7, seconds=-1)
                )


            # Prevent the end_date from going into the future
            if end_date_batch > end_date:
                end_date_batch = end_date

            # Convert the datetimes to datetime formats the api expects
            start_date_str: str = start_date_batch.format('Y-M-D')
            end_date_str: str = end_date_batch.format('Y-M-D')

            current_batch += 1

            self.logger.info(
                    f'Parsing batch {current_batch}: {start_date_str} <--> '
                    f'{end_date_str}',
                )

            params = self.get_url_params(context, next_page_token=None)

            self.logger.info(
                    f'Params: {params} '
                )

            
           

            params["filter[start_date]"] =  start_date_str
            params["filter[end_date]"] = end_date_str
            


            next_page = True
            while next_page:

              response: requests.Response = requests.get(
                  url=self.get_url(context),
                  headers=self.http_headers,
                  params=params
              )

            #   self.logger.info(
            #     f'Response: {response['X-RateLimit-Remaining']}'
            # )


              response.raise_for_status()

              response_data: dict = response.json()

              data: list = response_data.get("data", [])

              next_page_token = self.get_next_page_token(response, previous_token=None)

              if next_page_token:
                  params["next"] = next_page_token

              else:
                  next_page = False

              yield from (
                  completion for completion in data
                  )
    

    # def prepare_request_payload(self, context: Optional[dict], next_page_token: Optional[Any], **kwargs: dict) -> Generator[dict, None, None]:
    #     """ Get completions history.
        
    #     Raises: 
    #       ValueError: When parameter start_date is missing

    #     Yields:
    #       Generator[dict]: Yields Degreed completions over time range
        
    #     """

        

    #     #Validate start_date value
    #     start_date_input: str = str(self.config['start_date'])
    #     self.logger.info(f"Start date is {start_date_input}")

    #     if not start_date_input:
    #         raise ValueError('Parameter start_date is required.')
        
    #     # Set start date and end date
    #     start_date: datetime = pendulum.parse(start_date_input)
    #     end_date: datetime = pendulum.now('utc').replace(microsecond=0)

    #     self.logger.info(
    #         f"Retrieving completions from {start_date.format('Y-M-D')} to {end_date.format('Y-M-D')}"
    #     )

    #     # Extra kwargs will be converted to parameters in the API requests
    #     # start_date is parsed into batches, thus we remove it from the kwargs
    #     kwargs.pop('start_date', None)

    #     # Maximum date range between start_date and end_date is 7 days
    #     # Split requests into weekly batches
    #     batches: range = (pendulum.period(start_date, end_date).range('weeks'))

    #     current_batch: int = 0
    #     # Batches contain all start_dates, the end_date is 6 days 23:59 later
    #     # E.g. 2021-01-01T00:00:00+0000 <--> 2021-01-07T23:59:59+0000
    #     for start_date_batch in batches:
    #         end_date_batch: datetime = (
    #             start_date_batch.add(days=7, seconds=-1)
    #             )
            
            
    #         # Prevent the end_date from going into the future
    #         if end_date_batch > end_date:
    #             end_date_batch = end_date
            
    #         # Convert the datetimes to datetime formats the api expects
    #         start_date_str: str = start_date_batch.format('Y-M-D')
    #         end_date_str: str = end_date_batch.format('Y-M-D')
            
    #         current_batch += 1
        
    #         self.logger.info(
    #             f'Parsing batch {current_batch}: {start_date_str} <--> '
    #             f'{end_date_str}',
    #         )

    #         payload = {
    #             "limit": 1000,
    #             "filter[start_date]": start_date_str,
    #             "filter[end_date]": end_date_str

    #         }

    #         if next_page_token:
    #             payload["next"] = next_page_token
            
    #         return payload
        

        







# class AccomplishmentsStream(DegreedStream):
#     name = "accomplishments"
#     path = "/accomplishments"
#     primary_keys = ["id"]
#     replication_key = "completed-at"

#     schema = rearrange_schema(
#     PropertiesList(
#         Property("type", StringType),
#         Property("id", StringType),
#         Property("attributes", 
#                     ObjectType(
#                         Property("employee-id", StringType),
#                         Property("first-name", StringType),
#                         Property("last-name", StringType),
#                         Property("full-name", StringType),
#                         Property("organization-email", StringType),
#                         Property("personal-email", StringType),
#                         Property("profile-visibility", StringType),
#                         Property("bio", StringType),
#                         Property("location", StringType),
#                         Property("profile-image-url", StringType),
#                         Property("login-disabled", BooleanType),
#                         Property("restricted", BooleanType),
#                         Property("permission-role", StringType),
#                         Property("real-time-email-notification", BooleanType),
#                         Property("daily-digest-email", BooleanType),
#                         Property("weekly-digest-email", BooleanType),
#                         Property("created-at", DateTimeType),
#                         Property("first-login-at", DateTimeType),
#                         Property("last-login-at", DateTimeType),
#                         Property("total-points", NumberType),
#                         Property("daily-logins", IntegerType)
#                     )),
#         Property("relationships", 
#                     ArrayType(
#                         ObjectType(
#                             Property("manager", ObjectType(
#                                 Property("data", ObjectType(
#                                     Property("id", StringType),
#                                     Property("type", StringType)
#                                 ))
#                             ))

#                     )))
#     ).to_dict())

# class PathwaysStream(DegreedStream):
#     name = "pathways"
#     path = "/pathways"
#     primary_keys = ["id"]
#     replication_key = "modified-at"

#     schema = rearrange_schema(
#     PropertiesList(
#         Property("type", StringType),
#         Property("id", StringType),
#         Property("attributes", 
#                     ObjectType(
#                         Property("employee-id", StringType),
#                         Property("first-name", StringType),
#                         Property("last-name", StringType),
#                         Property("full-name", StringType),
#                         Property("organization-email", StringType),
#                         Property("personal-email", StringType),
#                         Property("profile-visibility", StringType),
#                         Property("bio", StringType),
#                         Property("location", StringType),
#                         Property("profile-image-url", StringType),
#                         Property("login-disabled", BooleanType),
#                         Property("restricted", BooleanType),
#                         Property("permission-role", StringType),
#                         Property("real-time-email-notification", BooleanType),
#                         Property("daily-digest-email", BooleanType),
#                         Property("weekly-digest-email", BooleanType),
#                         Property("created-at", DateTimeType),
#                         Property("first-login-at", DateTimeType),
#                         Property("last-login-at", DateTimeType),
#                         Property("total-points", NumberType),
#                         Property("daily-logins", IntegerType)
#                     )),
#         Property("relationships", 
#                     ArrayType(
#                         ObjectType(
#                             Property("manager", ObjectType(
#                                 Property("data", ObjectType(
#                                     Property("id", StringType),
#                                     Property("type", StringType)
#                                 ))
#                             ))

#                     )))
#     ).to_dict())
