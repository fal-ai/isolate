# This file defines an "isolate" agent for inter-process communication over
# sockets. It is spawned by the controller process with a single argument (a
# base64 encoded server address) and expected to go through the following procedures:
#   1. Decode the given address
#   2. Create a connection to the transmission bridge using the address
#   3. Receive a callable object from the bridge
#   4. Execute the callable object
#   5. Send the result back to the bridge
#   6. Exit
#
# Up until to point 4, the agent process has no way of transmitting information
# to the controller so it should use the stderr/stdout channels appropriately. After
# the executable is received, the controller process will switch to the listening mode
# and wait for agent to return something. The expected object is a tuple of two objects
# one being the actual result of the given callable, and the other one is a boolean flag
# indicating whether the callable has raised an exception or not.

from __future__ import annotations

import base64
import importlib
import os
import sys
import time
import traceback
from argparse import ArgumentParser
from contextlib import closing
from multiprocessing.connection import Client
from typing import TYPE_CHECKING, Any, Callable, ContextManager

if TYPE_CHECKING:
    # Somhow mypy can't figure out that `ConnectionWrapper`
    # really exists.
    class ConnectionWrapper:
        def __init__(
            self,
            connection: Any,
            loads: Callable[[bytes], Any],
            dumps: Callable[[Any], bytes],
        ) -> None: ...

        def recv(self) -> Any: ...

        def send(self, value: Any) -> None: ...

        def close(self) -> None: ...

else:
    from multiprocessing.connection import ConnectionWrapper


def decode_service_address(address: str) -> tuple[str, int]:
    host, port = base64.b64decode(address).decode("utf-8").rsplit(":", 1)
    return host, int(port)


def child_connection(
    serialization_method: str, address: tuple[str, int]
) -> ContextManager[ConnectionWrapper]:
    serialization_backend = importlib.import_module(serialization_method)
    return closing(
        ConnectionWrapper(
            Client(address),
            loads=serialization_backend.loads,
            dumps=serialization_backend.dumps,
        )
    )


IS_DEBUG_MODE = os.getenv("ISOLATE_ENABLE_DEBUGGING") == "1"
DEBUG_TIMEOUT = 60 * 15


def run_client(
    serialization_method: str,
    address: tuple[str, int],
    *,
    with_pdb: bool = False,
    log_fd: int | None = None,
) -> None:
    # Debug Mode
    # ==========
    #
    # Isolated processes are really tricky to debug properly
    # so we want to have a smooth way into the process and see
    # what is really going on in the case of errors.
    #
    # For using the debug mode, you first need to set ISOLATE_ENABLE_DEBUGGING
    # environment variable to "1" from your controller process. This will
    # make the isolated process hang at the initialization, and make it print
    # the instructions to connect to the controller process.
    #
    # On a separate shell (while letting the the controller process hang), you can
    # execute the given command to drop into the PDB (Python Debugger). With that
    # you can observe each step of the connection and run process.

    if with_pdb:
        # This condition will only be activated if we want to
        # debug the isolated process by passing the --with-pdb
        # flag when executing the binary.
        import pdb

        pdb.set_trace()

    if log_fd is None:
        _log = sys.stdout
    else:
        _log = os.fdopen(log_fd, "w")

    def log(_msg):
        _log.write(_msg)
        _log.flush()

    log(f"Trying to create a connection to {address}")
    # TODO(feat): this should probably run in a loop instead of
    # receiving a single function and then exitting immediately.
    with child_connection(serialization_method, address) as connection:
        log(f"Created child connection to {address}")
        callable = connection.recv()
        log(f"Received the callable at {address}")

        result = None
        did_it_raise = False
        stringized_tb = None
        try:
            result = callable()
        except BaseException as exc:
            result = exc
            did_it_raise = True
            num_frames = len(traceback.extract_stack()[:-4])
            stringized_tb = "".join(traceback.format_exc(limit=-num_frames))
        finally:
            try:
                connection.send((result, did_it_raise, stringized_tb))
            except BaseException:
                if did_it_raise:
                    # If we can't even send it through the connection
                    # still try to dump it to the stderr as the last
                    # resort.
                    assert isinstance(result, BaseException)
                    traceback.print_exception(
                        type(result),
                        result,
                        result.__traceback__,
                    )
                raise


def _get_shell_bootstrap() -> str:
    # Return a string that contains environment variables that
    # might be used during isolated hook's execution.
    return " ".join(
        f"{session_variable}={os.getenv(session_variable)}"
        for session_variable in [
            # PYTHONPATH is customized by the Extended Environment IPC
            # system to make sure that the isolated process can
            # import stuff from the primary environment. Without this
            # the isolated process will not be able to run properly
            # on the newly created debug session.
            "PYTHONPATH",
        ]
        if session_variable in os.environ
    )


def main() -> int:
    parser = ArgumentParser()
    parser.add_argument("listen_at")
    parser.add_argument("--with-pdb", action="store_true", default=False)
    parser.add_argument("--serialization-backend", default="pickle")
    parser.add_argument("--log-fd", type=int)

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        assert not options.with_pdb, "--with-pdb can't be used in the debug mode"
        message = "=" * 60
        message += "\n" * 3
        message += (
            "Debug mode successfully activated. "
            "You can start your debugging session with the following command:\n"
        )
        message += (
            f"    $ {_get_shell_bootstrap()}\\\n     "
            f"{sys.executable} {os.path.abspath(__file__)} "
            f"--serialization-backend {options.serialization_backend} "
            f"--with-pdb {options.listen_at}"
        )
        message += "\n" * 3
        message += "=" * 60
        print(message)
        time.sleep(DEBUG_TIMEOUT)

    serialization_method = options.serialization_backend
    address = decode_service_address(options.listen_at)
    run_client(
        serialization_method,
        address,
        with_pdb=options.with_pdb,
        log_fd=options.log_fd,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
