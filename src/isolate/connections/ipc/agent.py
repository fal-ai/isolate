import os
import sys
import time
from argparse import ArgumentParser

from isolate.connections.ipc import bridge

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


def run_client(address: str, *, with_pdb: bool = False) -> int:
    if with_pdb:
        # This condition will only be activated if we want to
        # debug the isolated process by passing the --with-pdb
        # flag when executing the binary.
        import pdb

        pdb.set_trace()

    print("Trying to create a connection to {}", address)
    with bridge.child_connection(address) as connection:
        print("Created child connection to {}", address)
        callable = connection.recv()
        print("Received the callable at {}", address)
        try:
            result = callable()
            exception = None
        except BaseException as exc:
            result = None
            exception = exc
        finally:
            connection.send((result, exception))
        return result


def _get_shell_bootstrap() -> str:
    # Return a string that contains environment variables that
    # might be used during isolated hook's execution.
    return " ".join(
        f"{session_variable}={os.getenv(session_variable)}"
        for session_variable in [
            # PYTHONPATH is customized by the Dual Environment IPC
            # system to make sure that the isolated process can
            # import stuff from the primary environment. Without this
            # the isolated process will not be able to run properly
            # on the newly created debug session.
            "PYTHONPATH",
        ]
        if session_variable in os.environ
    )


def main() -> int:
    print("Starting the isolated process at PID {}", os.getpid())

    parser = ArgumentParser()
    parser.add_argument("listen_at")
    parser.add_argument("--with-pdb", action="store_true", default=False)

    options = parser.parse_args()
    if IS_DEBUG_MODE:
        assert not options.with_pdb, "--with-pdb can't be used in the debug mode"
        message = "=" * 60
        message += "\n" * 3
        message += "Debug mode successfully activated. You can start your debugging session with the following command:\n"
        message += f"    $ {_get_shell_bootstrap()}\\\n     {sys.executable} {os.path.abspath(__file__)} --with-pdb {options.listen_at}"
        message += "\n" * 3
        message += "=" * 60
        print(message)
        time.sleep(DEBUG_TIMEOUT)

    address = bridge.decode_service_address(options.listen_at)
    run_client(address, with_pdb=options.with_pdb)
    return 0


if __name__ == "__main__":
    sys.exit(main())
