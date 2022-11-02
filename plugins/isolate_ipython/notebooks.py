import io
from types import FunctionType

from IPython.core.magic import (
    Magics,
    cell_magic,
    magics_class,
    needs_local_scope,
)
from IPython.core.magic_arguments import (
    argument,
    magic_arguments,
    parse_argstring,
)

from isolate.backends import BaseEnvironment
from isolate.backends.settings import IsolateSettings


def run_in_isolated_mode(
    environment: BaseEnvironment,
    buffer: io.BytesIO,
    cell: str,
) -> None:
    from functools import partial

    settings = IsolateSettings(serialization_method="dill")
    environment.apply_settings(settings)

    def load_session_from_buffer(buffer, cell):
        import io
        import sys
        import types

        import dill

        # Restore previous state
        original_main = sys.modules["__main__"]
        sys.modules["__main__"] = types.ModuleType("__main__")
        dill.load_session(buffer, sys.modules["__main__"])

        # Run the cell
        exec(cell, sys.modules["__main__"].__dict__)

        # Save the new state
        session_buffer = io.BytesIO()
        dill.dump_session(session_buffer, main=sys.modules["__main__"])
        session_buffer.seek(0)
        sys.modules["__main__"] = original_main

        return session_buffer

    with environment.connect() as connection:
        return connection.run(partial(load_session_from_buffer, buffer, cell))  # type: ignore


def split_hidden_entries(module):
    removed_objects = {}
    always_allow = {"__builtins__", "__name__", "__doc__", "__package__"}
    always_hide = {"In", "Out", "exit", "quit", "get_ipython", "open"}
    for name, obj in module.__dict__.copy().items():
        if name in always_allow:
            continue

        if name.startswith("_") or name in always_hide:
            removed_objects[name] = module.__dict__.pop(name)
            continue

        # Since the isolated environment should not change definitions
        # of existing environments, we need to remove all objects originating
        # from isolate-ipython.
        if isinstance(obj, (type, FunctionType)):
            obj_type = obj
        else:
            obj_type = type(obj)

        if obj_type.__module__.startswith("isolate"):
            removed_objects[name] = module.__dict__.pop(name)

    return removed_objects


def merge_hidden_entries(module, removed_objects):
    for name, obj in removed_objects.items():
        module.__dict__.setdefault(name, obj)


@magics_class
class IsolateRunner(Magics):
    @magic_arguments()
    @argument("environment_name", type=str, help="Name of the defined environment.")
    @needs_local_scope
    @cell_magic
    def isolated(self, line, cell, local_ns):
        import io

        import __main__
        import dill

        args = parse_argstring(self.isolated, line)
        environment_definition = local_ns.get(args.environment_name)
        if environment_definition is None:
            raise ValueError(f"Environment {args.environment_name} is not defined.")

        environment = environment_definition.prepare()

        # Remove all the isolate environment definitions (e.g. Machine, Environment)
        hidden_entries = split_hidden_entries(__main__)
        try:
            # Save the previous state
            dill.settings["recurse"] = True
            dill.settings["byref"] = True
            session_buffer = io.BytesIO()
            dill.dump_session(session_buffer, main=__main__)
            session_buffer.seek(0)

            # Run the cell in the environment.
            result = run_in_isolated_mode(environment, session_buffer, cell)
        finally:
            merge_hidden_entries(__main__, hidden_entries)

        if result is None:
            raise ValueError("Unexpected problem: no result was returned?")
        else:
            dill.load_session(result, main=__main__)
