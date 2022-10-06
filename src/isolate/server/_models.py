from typing import Type, TypeVar

from marshmallow import Schema, fields, validate

T = TypeVar("T")


def with_schema(schema_type: Type[Schema], data: T) -> T:
    schema = schema_type()
    return schema.load(data)


class EnvironmentRun(Schema):
    environment_token = fields.String(required=True)
    serialization_backend = fields.String(required=True)


class Environment(Schema):
    kind = fields.String(
        validate=validate.OneOf(["conda", "virtualenv"]),
        required=True,
    )
    configuration = fields.Dict(required=True)


class StatusRequest(Schema):
    # Denotes the index of the logs that the client has
    # already received up to.
    logs_start = fields.Integer(load_default=0)
