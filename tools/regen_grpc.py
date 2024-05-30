import ast
import glob
import os
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

from refactor import Rule, Session, actions

PROJECT_ROOT = Path(__file__).parent.parent / "src"
COMMON_DIR = PROJECT_ROOT / "isolate" / "connections" / "grpc" / "definitions"
KNOWN_PATHS = {"common_pb2": "isolate.connections.grpc.definitions"}


class FixGRPCImports(Rule):
    """Change all unqualified imports to qualified imports."""

    def match(self, node: ast.AST) -> actions.Replace:
        # import *_pb2
        assert isinstance(node, ast.Import)
        assert len(node.names) == 1
        assert not node.names[0].name.startswith("google")
        assert node.names[0].name.endswith("_pb2")

        # If we know where the import is coming from, use that.
        qualified_name = KNOWN_PATHS.get(node.names[0].name)

        if not qualified_name:
            # Otherwise discover it from the current file path.
            parent_dir = self.context.file.resolve().relative_to(PROJECT_ROOT).parent
            qualified_name = ".".join(parent_dir.parts)

        # Change import *_pb2 to from <qualified_name> import *_pb2
        return actions.Replace(
            node,
            ast.ImportFrom(module=qualified_name, names=node.names, level=0),
        )


def regen_grpc(file: Path) -> None:
    assert file.exists()

    parent_dir = file.parent
    common_dir = os.path.relpath(COMMON_DIR, parent_dir)
    subprocess.check_output(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            f"-I={common_dir}",
            "--proto_path=.",
            "--python_out=.",
            "--grpc_python_out=.",
            "--mypy_out=.",
            file.name,
        ],
        cwd=parent_dir,
    )

    # Python gRPC compiler is bad at using the proper import
    # notation so it doesn't work with our package structure.
    #
    # See: https://github.com/protocolbuffers/protobuf/issues/1491

    # For fixing this we are going to manually correct the generated
    # source.
    for grpc_output_file in parent_dir.glob("*_pb2*.py*"):
        session = Session(rules=[FixGRPCImports])
        changes = session.run_file(grpc_output_file)
        if changes:
            changes.apply_diff()


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("definition_file", nargs="?")

    options = parser.parse_args()

    if options.definition_file:
        files = [options.definition_file]
    else:
        files = glob.glob("**/*.proto", recursive=True)
        if not files:
            raise Exception("No definition files specified or found.")

    for file in files:
        regen_grpc(Path(file))


if __name__ == "__main__":
    main()
