import os
import secrets
import sys
import threading
import traceback
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, List, Optional

from isolate.backends import (
    BaseEnvironment,
    EnvironmentCreationError,
    UserException,
)
from isolate.backends.common import run_serialized
from isolate.backends.connections import ExtendedPythonIPC
from isolate.backends.context import Log, LogLevel, LogSource

# TODO: This is currently a server-level setting, but if it makes sense
# we can add it as a configuration variable to venv/conda etc (all the
# environments that use Python IPC).
INHERIT_FROM_LOCAL = os.getenv("ISOLATE_INHERIT_FROM_LOCAL") == "1"

# This is only used to serialize the partial function.
SECONDARY_SERIALIZATION_METHOD = "dill"


def run_serialized_function_in_env(
    environment: BaseEnvironment,
    serialized_function: bytes,
    done_event: threading.Event,
) -> None:
    """Run the given function (serialized with the serialization method)"""
    try:
        try:
            env_path = environment.create()
        except EnvironmentCreationError:
            environment.log(
                "Failed to create the environment. Aborting the run.",
                level=LogLevel.ERROR,
                source=LogSource.BUILDER,
            )
            raise

        inherit_from = []
        if INHERIT_FROM_LOCAL:
            # This assumes that the inherited environment complies with the
            # sysconfig, but Debian is a notorious outlier so it should be noted
            # that this won't work with 'system Python' on debian based systems.
            #
            # You can still inherit from a venv you activated to run the isolate
            # server though (even on debian). Always use virtual environments.
            local_env = Path(sys.exec_prefix)
            inherit_from.append(local_env)

        with ExtendedPythonIPC(
            environment,
            env_path,
            inherit_from,
        ) as connection:
            callable = partial(
                run_serialized,
                SECONDARY_SERIALIZATION_METHOD,
                serialized_function,
            )
            return connection.run(
                callable,
                ignore_exceptions=True,
            )
    finally:
        done_event.set()
