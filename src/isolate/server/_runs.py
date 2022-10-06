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


@dataclass
class RunInfo:
    token: str = field(default_factory=secrets.token_urlsafe)
    logs: List[Log] = field(default_factory=list)

    bound_thread: Optional[threading.Thread] = None
    done_signal: threading.Event = field(default_factory=threading.Event)

    @property
    def is_done(self) -> bool:
        return self.done_signal.is_set()


def dehydrated_dual_env_run(
    environment: BaseEnvironment,
    data: bytes,
    done_event: threading.Event,
) -> None:
    try:
        try:
            env_path = environment.create()
        except EnvironmentCreationError:
            environment.log(
                "Failed to create the environment. Aborting the run.",
                level=LogLevel.ERROR,
                source=LogSource.BUILDER,
            )

            # Probably something wrong with the environment definition (e.g
            # pip installing something that is not on PyPI), so we don't need
            # to crash the whole application isolate server.
            return None

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
            # TODO: we currently discard the result, but maybe we should stream
            # it back somehow to the client if they ask and they can handle the
            # decoding.
            result: Any = connection.run(
                partial(run_serialized, "dill", data), ignore_exceptions=True
            )

        # If the user gets an error, currently all we do is just log it as if
        # it was printed to stderr and exit.
        if isinstance(result, UserException):
            exception = result.exception
            for line in traceback.format_exception(
                type(exception), exception, exception.__traceback__
            ):
                connection.log(line, level=LogLevel.STDERR, source=LogSource.USER)

    finally:
        done_event.set()
