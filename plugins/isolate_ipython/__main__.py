import copy
import sys
import tempfile
import textwrap
from argparse import ArgumentParser
from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Optional

import isolate

# Assume this comes from fal_project.yml
DEFINITIONS = {
    "ml": {
        "kind": "virtualenv",
        "requirements": [
            "pyjokes",
        ],
    }
}


def get_ipython_config(default_env: Dict[str, Any]) -> Path:
    config_dir = Path(tempfile.mkdtemp())
    default_profile_dir = config_dir / "profile_default"
    default_profile_dir.mkdir(parents=True, exist_ok=True)

    default_profile_dir = default_profile_dir / "ipython_config.py"
    default_profile_dir.write_text(
        textwrap.dedent(
            f"""\
                c.InteractiveShellApp.extensions.append('isolate_ipython')
                c.InteractiveShellApp.exec_lines = [
                    "_global_isolate_env={repr(default_env)}"
                ]
    """
        )
    )
    return config_dir


def create_notebook(name: str) -> None:
    definition = copy.deepcopy(DEFINITIONS[name])
    ipython_config = get_ipython_config(definition)

    if definition["kind"] == "virtualenv":
        definition["requirements"].extend(["jupyterlab", "."])  # type: ignore
    else:
        raise NotImplementedError(definition["kind"])

    environment = isolate.prepare_environment(**definition)
    with environment.connect() as connection:
        connection.run(
            partial(
                exec,
                "import os;"
                "import sys;"
                "from jupyterlab.labapp import main;"
                "sys.argv = ['jupyter'];"
                f"os.environ['IPYTHONDIR'] = '{ipython_config}';"
                "main()",
            )
        )


def main(argv: Optional[List[str]] = None) -> int:
    parser = ArgumentParser()
    subparsers = parser.add_subparsers(dest="action")

    create_parser = subparsers.add_parser("create")
    create_parser.add_argument("name")

    options = parser.parse_args()
    if options.action == "create":
        create_notebook(options.name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
