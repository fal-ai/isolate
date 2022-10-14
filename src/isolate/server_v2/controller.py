import secrets
import threading
from concurrent import futures
from dataclasses import dataclass, field
from queue import Empty as QueueEmpty
from queue import Queue
from typing import Any, ContextManager, Generic, Iterator, Tuple, TypeVar

import grpc
from grpc import ServicerContext

from isolate.backends import (
    BasicCallable,
    CallResultType,
    EnvironmentConnection,
)
from isolate.server_v2 import definitions

# Number of seconds to wait for agent to connect to the controller through gRPC. This allows
# us to see if for example the underlying agent died before even making the connection, we can
# simply abort instead of waiting indefinitely for it.
MAX_CONNECTION_TIMEOUT = 2.5

# Maximum number of seconds an individual run can take. Currently 24 hours.
MAX_RUN_TIME = 60 * 60 * 24

T = TypeVar("T")


class AgentError(Exception):
    """An internal problem caused by (most probably) the agent."""


@dataclass
class SingleUseBridge(definitions.ControllerBridgeServicer):
    function: definitions.SerializedObject
    partial_results: Queue[definitions.PartialRunResult] = field(default_factory=Queue)
    established: threading.Event = field(default_factory=threading.Event)

    def Connect(
        self,
        request_iterator: Iterator[definitions.SerializedObject],
        context: ServicerContext,
    ) -> Iterator[definitions.Task]:
        self.established.set()

        yield definitions.Task(function=self.function)
        for result in request_iterator:
            self.result_queue.put_nowait(result)


@dataclass
class RemotePythonConnection(Generic[T], EnvironmentConnection):
    """A gRPC based connection system to allow proxying incoming
    messages from one node to another in the form of a structured
    SerializedObject."""

    def start_agent(
        self,
        address: str,
        trigger_event: threading.Event,
    ) -> ContextManager[Any]:
        """Start the agent and instruct it to connect to the bridge
        that is currently running on the given address. This function
        also receives a threading.Event, which should be set to true
        in the event of agent failing to start up (e.g. if this method
        is creating a process, and if that process dies before connecting
        to the bridge, it should set the given trigger_event)."""
        raise NotImplementedError

    def create_server(self, host: str, port: int) -> Tuple[grpc.Server, str, str]:
        """Create a new (temporary) gRPC server on the given host/port combination.
        Return the server, the address, and the access token."""
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=1),
            maximum_concurrent_rpcs=1,
        )

        access_token = secrets.token_hex(16)
        server_credentials = grpc.access_token_call_credentials(access_token)

        # The port might have changed (e.g. if it is 0, gRPC will automatically assign
        # us a free port number which we can use it to obtain the new address).
        port = server.add_secure_port(f"{host}:{port}", server_credentials)
        return server, f"{host}:{port}", access_token

    def prepare_bridge(
        self, function: definitions.SerializedObject, server: grpc.Server
    ) -> SingleUseBridge:
        """Return a new, single-use local controller bridge for the
        given serialized object on the given server."""
        bridge = SingleUseBridge(function=function)

        # This function just calls some methods on the server
        # and register a generic handler for the bridge. It does
        # not have any global side effects.
        definitions.register_controller_bridge(bridge, server)
        return bridge

    def proxy_grpc(
        self,
        function: definitions.SerializedObject,
        host: str = "[::]",
        port: int = 0,
    ) -> definitions.SerializedObject:
        """Send the given 'function' to the agent process, and return the
        result back. This method creates a new server on the fly (with the
        given host/port combination), so it should be ensured that the given
        address is accessible from the agent (if they are in different machines,
        the network access to the given port should be allowed).

        If the agent can't start, this function might raise a TimeoutError."""

        # Implementation details
        # ======================
        #
        #  RPC Flow:
        #  ---------
        #  1. [controller]: Start the gRPC Bridge server.
        #  2. [controller]: Start the agent with the address and the access token.
        #  3.      [agent]: Connect to the bridge using the access token.
        #  4. [controller]: Await *at most* MAX_CONNECTION_TIMEOUT seconds for the
        #                   agent to establish a connection, if it doesn't do it until
        #                   then, raise an AgentError.
        #  5. [controller]: If the connection is established, then send the function.
        #  6.      [agent]: Receive the function, deserialize it, start the execution.
        #  7. [controller]: Watch agent for logs (stdout/stderr), and as soon as they appear
        #                   call the log handler.
        #  8.      [agent]: Once the execution of the function is finished, send the result
        #                   using the same serialization method.
        #  9. [controller]: Receive the result back (from the result_queue), and as soon as
        #                   it indicates the execution is completed, stop the bridge and the
        #                   agent.

        server, address, access_token = self.create_server(host=host, port=port)
        bridge: SingleUseBridge = self.prepare_bridge(server)

        server.start()
        if not bridge.established.wait(timeout=MAX_CONNECTION_TIMEOUT):
            raise AgentError(
                f"Agent failed to connect to the bridge "
                f"within the {MAX_CONNECTION_TIMEOUT} seconds."
            )

        # As soon as the bridge is established, we can stop accepting new requests
        # and ask the gRPC to let us know as soon as the existing connection is done.
        is_done_event: threading.Event = server.stop(grace=MAX_RUN_TIME)
        while not is_done_event.is_set():
            try:
                # We have to use timeout here, because otherwise this would block
                # indefinietly (even if the underlying connection is closed). Having
                # a timeout allows us to regularly check whether the gRPC server is
                # still running (which means the agent is still running and producing
                # results).
                partial_result = bridge.partial_results.get(timeout=0.1)
            except QueueEmpty:
                continue

            # We can safely exit if we know for a fact that the
            # agent has finished its execution.
            if partial_result.is_complete:
                return partial_result.result

        raise AgentError("Agent failed to produce any results back.")

    def run(
        self,
        executable: BasicCallable,
        ignore_exceptions: bool = False,
        *args: Any,
        **kwargs: Any,
    ) -> CallResultType:
        # TODO: this should build a gRPC Message, use proxy_grpc, and then deserialize
        # the result back.
        raise NotImplementedError
