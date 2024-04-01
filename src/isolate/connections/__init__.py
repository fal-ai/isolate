import importlib

from isolate.connections.ipc import IsolatedProcessConnection, PythonIPC  # noqa: F401


def __getattr__(name):
    if name == "LocalPythonGRPC":
        extra = "grpc"
        module_name = "isolate.connections.grpc"
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise AttributeError(
            f"For using {name!r} you need to install isolate with {extra!r} support."
            f'\n    $ pip install "isolate[{extra}]"'
        )

    return getattr(module, name)
