# This module is split into two parts:
#   1) A core serialization library that both the controller and the agent
#      share (since the agent process can run in an environment that does
#      not necessarily have 'isolate', we can't simply import it from a common
#      module).
#
#   2) Implementation of the agent itself.
#
# Skip to the bottom of the file to see the agent implementation.


# WARNING: Please do not import anything outside of standard library before
# the call to load_pth_files(). After that point, imports to installed packages
# are allowed.

from __future__ import annotations

import base64
import importlib
import json
import os
import site
import sys
import time
import traceback
from argparse import ArgumentParser
from contextlib import closing, contextmanager
from dataclasses import dataclass
from functools import partial
from multiprocessing.connection import Client, ConnectionWrapper
from typing import (
    TYPE_CHECKING,
    Any,
    ContextManager,
    Dict,
    Iterator,
    Tuple,
    cast,
)

if TYPE_CHECKING:
    from typing import Protocol

    class SerializationBackend(Protocol):
        def loads(self, data: bytes) -> Any:
            ...

        def dumps(self, obj: Any) -> bytes:
            ...


# Common IPC Protocol
# ===================
#
# Isolate's IPC protocol is currently structured as a communication
# of serialized objects (no log transfer is happening over the IPC,
# but rather is is the responsibility of the controller process to
# watch the stdout/stderr of the isolated process).
#
# Each serialized object is a JSON message with the following schema
# {
#   "serialization_method": [string],
#   "raw_object": [string],
#   "was_raised": [bool],
# }
#
# The "raw_object" is basically a Python object serialized using the
# given serialization method and encoded with base64 (since JSON does
# not support binary data).
#
# The "was_raised" is a flag that indicates whether the given object
# was raised or returned (return Exception() or raise Exception()).
#


@dataclass
class SerializedObject:
    """SerializedObject is a common form of data that can be passed
    between the agent and the controller without requiring Python-native
    serialization (pickle, dill, etc.). This is primarily useful for workloads
    where the controller might not have packages installed to support deserialization
    of the either the input callable or the resulting object (e.g. isolate server)."""

    serialization_method: str
    object: Any
    was_raised: bool = False

    def to_json_message(self) -> Dict[str, Any]:
        raw_object = serialize_object(self.serialization_method, self.object)
        return {
            "serialization_method": self.serialization_method,
            "raw_object": base64.b64encode(raw_object).decode(),
            "was_raised": self.was_raised,
        }

    @classmethod
    def from_json_message(cls, message: Dict[str, Any]) -> SerializedObject:
        with _step("Unpacking the given JSON message"):
            method = message["serialization_method"]
            was_raised = message["was_raised"]
            raw_object = base64.b64decode(message["raw_object"])

        return cls(
            method,
            load_serialized_object(method, raw_object),
            was_raised,
        )


class SerializationError(Exception):
    """An error that happened during the serialization process."""


@contextmanager
def _step(message: str) -> Iterator[None]:
    """A context manager to capture every expression
    underneath it and if any of them fails for any reason
    then it will raise a SerializationError with the
    given message."""

    try:
        yield
    except BaseException as exception:
        raise SerializationError(message) from exception


def as_serialization_backend(backend: Any) -> SerializationBackend:
    """Ensures that the given backend has loads/dumps methods, and returns
    it as is (also convinces type checkers that the given object satisfies
    the serialization protocol)."""

    if not hasattr(backend, "loads") or not hasattr(backend, "dumps"):
        raise TypeError(
            f"The given serialization backend ({backend.__name__}) does "
            "not have one of the required methods (loads/dumps)."
        )

    return cast(SerializationBackend, backend)


def load_serialized_object(serialization_method: str, raw_object: bytes) -> Any:
    """Load the given serialized object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    with _step(f"Preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_backend(
            importlib.import_module(serialization_method)
        )

    with _step("Deserializing the given object"):
        return serialization_backend.loads(raw_object)


def serialize_object(serialization_method: str, object: Any) -> bytes:
    """Serialize the given object using the given serialization method. If
    anything fails, then a SerializationError will be raised."""

    with _step(f"Preparing the serialization backend ({serialization_method})"):
        serialization_backend = as_serialization_backend(
            importlib.import_module(serialization_method)
        )

    with _step("Deserializing the given object"):
        return serialization_backend.dumps(object)


def serialize_message(object: Any) -> bytes:
    """Serialize the given object (which should be a SerializedObject, otherwise
    fails with a TypeError) in the message form."""
    if not isinstance(object, SerializedObject):
        raise TypeError(f"Expected a SerializedObject, but got {type(object).__name__}")

    return json.dumps(object.to_json_message()).encode()


def deserialize_message(message: bytes) -> SerializedObject:
    """Deserialize the given message into a SerializedObject. The message must be
    a JSON object with the proper schema."""
    return SerializedObject.from_json_message(json.loads(message.decode()))


# Agent Implementation
# ====================
#
# This part defines an "isolate" agent for inter-process communication over
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
IS_DEBUG_MODE = os.getenv("ISOLATE_ENABLE_DEBUGGING") == "1"
DEBUG_TIMEOUT = 60 * 15


def run_client(address: Tuple[str, int], *, with_pdb: bool = False) -> None:
    if with_pdb:
        # This condition will only be activated if we want to
        # debug the isolated process by passing the --with-pdb
        # flag when executing the binary.
        import pdb

        pdb.set_trace()

    print(f"[trace] Trying to create a connection to {address}")
    # TODO(feat): this should probably run in a loop instead of
    # receiving a single function and then exitting immediately.
    with child_connection(address) as connection:
        print(f"[trace] Created child connection to {address}")
        try:
            incoming_msg: SerializedObject = connection.recv()
        except SerializationError as exception:
            traceback.print_exc()
            print(
                "[trace] Failed to receive the callable (due to a deserialization error). "
                "Aborting the run now."
            )
            return 1

        print(f"[trace] Received the callable at {address}")
        was_raised = False
        try:
            result = incoming_msg.object()
        except BaseException as exc:
            result = exc
            was_raised = True

        outgoing_msg = SerializedObject(
            serialization_method=incoming_msg.serialization_method,
            object=result,
            was_raised=was_raised,
        )

        try:
            connection.send(outgoing_msg)
        except SerializationError:
            traceback.print_exc()
            print(
                "[trace] Error while serializing the result back to the controller. "
                "Aborting the run now."
            )
            return 1
        except BaseException:
            traceback.print_exc()
            print(
                "[trace] Unknown error while sending the result back to the controller. "
                "Aborting the run now."
            )
            return 1


def decode_service_address(address: str) -> Tuple[str, int]:
    host, port = base64.b64decode(address).decode("utf-8").rsplit(":", 1)
    return host, int(port)


def child_connection(address: Tuple[str, int]) -> ContextManager[ConnectionWrapper]:
    return closing(
        ConnectionWrapper(
            Client(address),
            loads=deserialize_message,
            dumps=serialize_message,
        )
    )


def load_pth_files() -> None:
    """Each site dir in Python can contain some .pth files, which are
    basically instructions that tell Python to load other stuff. This is
    generally used for editable installations, and just setting PYTHONPATH
    won't make them expand so we need manually process them. Luckily, site
    module can simply take the list of new paths and recognize them.

    https://docs.python.org/3/tutorial/modules.html#the-module-search-path
    """
    python_path = os.getenv("PYTHONPATH")
    if python_path is None:
        return None

    # TODO: The order here is the same as the one that is used for generating the
    # PYTHONPATH. The only problem that might occur is that, on a chain with
    # 3 ore more nodes (A, B, C), if X is installed as an editable package to
    # B and a normal package to C, then C might actually take precedence. This
    # will need to be fixed once we are dealing with more than 2 nodes and editable
    # packages.
    for site_dir in python_path.split(os.pathsep):
        site.addsitedir(site_dir)


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
    print(f"[trace] Starting the isolated process at PID {os.getpid()}")

    parser = ArgumentParser()
    parser.add_argument("listen_at")
    parser.add_argument("--with-pdb", action="store_true", default=False)

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        assert not options.with_pdb, "--with-pdb can't be used in the debug mode"
        message = "=" * 60
        message += "\n" * 3
        message += "Debug mode successfully activated. You can start your debugging session with the following command:\n"
        message += (
            f"    $ {_get_shell_bootstrap()}\\\n     "
            f"{sys.executable} {os.path.abspath(__file__)} "
            f"--with-pdb {options.listen_at}"
        )
        message += "\n" * 3
        message += "=" * 60
        print(message)
        time.sleep(DEBUG_TIMEOUT)
        return 1

    address = decode_service_address(options.listen_at)
    run_client(address, with_pdb=options.with_pdb)
    return 0


if __name__ == "__main__":
    load_pth_files()
    sys.exit(main())
