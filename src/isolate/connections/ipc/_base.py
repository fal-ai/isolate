import subprocess
from contextlib import ExitStack
from dataclasses import dataclass
from typing import Any, ContextManager

from isolate._base import BasicCallable, CallResultType, EnvironmentConnection
from isolate.connections.ipc import agent, bridge


@dataclass
class IsolatedProcessConnection(EnvironmentConnection):
    def start_process(
        self, connection: bridge.ConnectionWrapper, *args: Any, **kwargs: Any
    ) -> ContextManager[subprocess.Popen]:
        """Start the agent process."""
        raise NotImplementedError

    def run(
        self, executable: BasicCallable, *args: Any, **kwargs: Any
    ) -> CallResultType:
        """Spawn an agent process using the given environment, run the given
        `executable` in that process, and return the result object back."""

        with ExitStack() as stack:
            # IPC flow is the following:
            #  1. [controller]: Create the socket server
            #  2. [controller]: Spawn the call agent with the socket address
            #  3.      [agent]: Connect to the socket server
            #  4. [controller]: Accept the incoming connection request
            #  5. [controller]: Send the executable over the established bridge
            #  6.      [agent]: Receive the executable from the bridge
            #  7.      [agent]: Execute the executable and once done send the result back
            #  8. [controller]: Loop until either the isolated process exits or sends any
            #                   data (will be interpreted as a tuple of two mutually exclusive
            #                   objects, either a result object or an exception to be raised).
            #

            self.log("Starting the controller bridge.")
            controller_service = stack.enter_context(bridge.controller_connection())

            self.log(
                "Controller server is listening at {}. Attempting to start the agent process.",
                controller_service.address,
            )
            isolated_process = stack.enter_context(
                self.start_process(controller_service, *args, **kwargs)
            )

            self.log(
                "Awaiting agent process of {} to establish a connection.",
                isolated_process.pid,
            )
            established_connection = stack.enter_context(controller_service.accept())

            self.log(
                "Bridge between controller and the agent has been established.",
                controller_service.address,
            )
            established_connection.send(executable)

            self.log(
                "Executable has been sent, awaiting execution result.",
            )
            return self.poll_until_result(isolated_process, established_connection)

    def poll_until_result(
        self, process: subprocess.Popen, connection: bridge.ConnectionWrapper
    ) -> CallResultType:
        """Take the given process, and poll until either it exits or returns
        a result object."""

        while process.poll() is None:
            # Normally, if we do connection.read() without having this loop
            # it is going to block us indefinitely (even if the underlying
            # process has crashed). We can use a combination of process.poll
            # and connection.poll to check if the process is alive and has data
            # to move forward.
            if not connection.poll():
                continue

            result, exception = connection.recv()
            if exception is None:
                return result
            else:
                raise exception

        raise OSError("The isolated process has exited unexpectedly with code '{}'.")
