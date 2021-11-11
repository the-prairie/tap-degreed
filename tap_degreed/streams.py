"""Stream type classes for tap-degreed."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable


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

    # schema = rearrange_schema(
    # PropertiesList(
    #     Property("type", StringType),
    #     Property("id", StringType),
    #     Property("attributes", 
    #                 ObjectType(
    #                     Property("employee-id", StringType),
    #                     Property("completed-at", DateTimeType),
    #                     Property("added-at", DateTimeType),
    #                     Property("points-earned", NumberType),
    #                     Property("is-verified", BooleanType),
    #                     Property("rating", IntegerType),
    #                     Property("access-method", StringType),
    #                 )),
    #     Property("relationships", 
    #              ArrayType(ObjectType(
    #                  Property("content", ObjectType(
    #                      Property("data", ObjectType(
    #                          Property("id", StringType),
    #                          Property("type", StringType)
    #             ))
    #         ))
    #     )),
    #              ArrayType(ObjectType(
    #                  Property("provider", ObjectType(
    #                      Property("data", ObjectType(
    #                          Property("id", StringType),
    #                          Property("type", StringType)
    #             ))
    #         ))
    #     )),
    #             ArrayType(ObjectType(
    #                  Property("user", ObjectType(
    #                      Property("data", ObjectType(
    #                          Property("id", StringType),
    #                          Property("type", StringType)
    #             ))
    #         ))
    #     )),
    #     ),
    #     Property("included", ArrayType(ObjectType(
    #         Property("type", StringType),
    #         Property("id", StringType),
    #         Property("attributes", ObjectType(
    #             Property("content-type", StringType),
    #             Property("url", StringType),
    #             Property("title", StringType),
    #             Property("provider", StringType),
    #             Property("content-duration", NumberType),
    #             Property("content-duration-type", StringType),
    #             Property("is-internal", BooleanType)))
    #     )))
                
        

    # ).to_dict())

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["filter[start_date]"] = self.config["start_date"]
        params["filter[end_date]"] = self.config["start_date"]

        return params
        







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
