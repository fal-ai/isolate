import ast
import os

_GRPC_OPTION_PREFIX = "ISOLATE_GRPC_CALL_"


def get_default_options():
    """Return the default list of GRPC call options (both for
    server and client) which are set via environment variables.

    Each environment variable starting with `ISOLATE_GRPC_CALL_`
    will be converted to a GRPC option. The name of the option
    will be the name of the environment variable, with the
    `ISOLATE_GRPC_CALL_` prefix removed and converted to lowercase.
    """

    options = []
    for raw_key, raw_value in os.environ.items():
        if raw_key.startswith(_GRPC_OPTION_PREFIX):
            field = raw_key[len(_GRPC_OPTION_PREFIX) :].lower()
            value = ast.literal_eval(raw_value)
            options.append((f"grpc.{field}", value))
    return options
