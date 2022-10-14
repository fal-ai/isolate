import ast
import subprocess
import sys
from argparse import ArgumentParser
from pathlib import Path

from refactor import Rule, Session, actions

PROJECT_ROOT = Path(__file__).parent.parent / "src"


class FixGRPCImports(Rule):
    """Change all unqualified imports to qualified imports."""

    def match(self, node: ast.AST) -> actions.Replace:
        # import *_pb2
        assert isinstance(node, ast.Import)
        assert len(node.names) == 1
        assert node.names[0].name.endswith("_pb2")

        # from <actual_pkg> import *_pb2
        parent_dir = self.context.file.resolve().relative_to(PROJECT_ROOT).parent
        qualified_name = ".".join(parent_dir.parts)
        return actions.Replace(
            node,
            ast.ImportFrom(module=qualified_name, names=node.names, level=0),
        )


def regen_grpc(file: Path) -> None:
    assert file.exists()

    parent_dir = file.parent
    subprocess.check_output(
        [
            sys.executable,
            "-m",
            "grpc_tools.protoc",
            "--proto_path=.",
            "--python_out=.",
            "--grpc_python_out=.",
            "--pyi_out=.",
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
    parser.add_argument("definition_file", type=Path)

    options = parser.parse_args()
    regen_grpc(options.definition_file)


if __name__ == "__main__":
    main()
