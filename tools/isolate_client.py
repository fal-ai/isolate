from __future__ import annotations

import argparse
import sys

import grpc

from isolate.connections.common import SerializationError
from isolate.connections.grpc.interface import to_serialized_object
from isolate.server import definitions

environment = definitions.EnvironmentDefinition(kind="local")


def func_to_submit() -> str:
    import time

    print("Task started, sleeping for 10 seconds...")
    time.sleep(10)
    return "hello"


def teardown_func() -> None:
    print("Teardown function called")


def describe_rpc_error(error: grpc.RpcError) -> str:
    detail = error.details() if hasattr(error, "details") else ""
    if detail:
        return detail
    try:
        code = error.code()
        code_name = code.name if hasattr(code, "name") else str(code)
    except Exception:
        code_name = "UNKNOWN"
    return f"{code_name}: {error}"


class ClientApp:
    def __init__(
        self,
        stub: definitions.IsolateStub,
    ) -> None:
        self.stub = stub

    def run(self) -> None:
        while True:
            try:
                choice = (
                    input(
                        "\nChoose an action: submit [s], list [l], "
                        "cancel [c], metadata [m], quit [q]: "
                    )
                    .strip()
                    .lower()
                )
            except EOFError:
                print()
                break
            if choice in {"q", "quit"}:
                break
            if choice in {"s", "submit"}:
                self.handle_submit()
            elif choice in {"l", "list"}:
                self.handle_list()
            elif choice in {"c", "cancel"}:
                self.handle_cancel()
            else:
                print("Unknown choice.", file=sys.stderr)

    def handle_submit(self) -> None:
        try:
            serialized_function = to_serialized_object(
                func_to_submit,
                method="dill",
            )
        except SerializationError as exc:
            print(f"Failed to serialize the function: {exc.message}", file=sys.stderr)
            return
        except Exception as exc:
            print(f"Failed to serialize the function: {exc}", file=sys.stderr)
            return
        try:
            serialized_teardown_function = to_serialized_object(
                teardown_func,
                method="dill",
            )
        except SerializationError as exc:
            print(
                f"Failed to serialize the teardown function: {exc.message}",
                file=sys.stderr,
            )
            return
        bound = definitions.BoundFunction(
            function=serialized_function,
            teardown_func=serialized_teardown_function,
            environments=[environment],
        )
        request = definitions.SubmitRequest(function=bound)
        try:
            response = self.stub.Submit(request)
        except grpc.RpcError as exc:
            print(f"Submit failed: {describe_rpc_error(exc)}", file=sys.stderr)
            return
        print(f"Task submitted with id: {response.task_id}")

    def handle_list(self) -> None:
        try:
            response = self.stub.List(definitions.ListRequest())
        except grpc.RpcError as exc:
            print(f"List failed: {describe_rpc_error(exc)}", file=sys.stderr)
            return
        if not response.tasks:
            print("No active tasks.")
            return
        print("Active task ids:")
        for info in response.tasks:
            print(f"  {info.task_id}")

    def handle_cancel(self) -> None:
        try:
            task_id = input("Task id to cancel: ").strip()
        except EOFError:
            print()
            return
        if not task_id:
            print("Task id is required.", file=sys.stderr)
            return
        request = definitions.CancelRequest(task_id=task_id)
        try:
            self.stub.Cancel(request)
        except grpc.RpcError as exc:
            print(f"Cancel failed: {describe_rpc_error(exc)}", file=sys.stderr)
            return
        print(f"Cancellation requested for task {task_id}.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactive client for isolate.server."
    )
    parser.add_argument(
        "--host",
        default="localhost:50001",
        help="gRPC host of the isolate server.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    channel = grpc.insecure_channel(args.host)
    stub = definitions.IsolateStub(channel)
    client = ClientApp(stub=stub)
    try:
        client.run()
    except KeyboardInterrupt:
        print()
    finally:
        channel.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
