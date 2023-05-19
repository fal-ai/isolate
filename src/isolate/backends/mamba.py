from __future__ import annotations

import copy
import functools
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Union

from isolate.backends import BaseEnvironment, EnvironmentCreationError
from isolate.backends.common import (
    active_python,
    logged_io,
    optional_import,
    sha256_digest_of,
)
from isolate.backends.settings import DEFAULT_SETTINGS, IsolateSettings
from isolate.connections import PythonIPC
from isolate.logs import LogLevel

# Specify the path where the mamba binary might reside in (or
# mamba, if it is the preferred one).
_MAMBA_COMMAND = os.environ.get("MAMBA_EXE", "micromamba")
_ISOLATE_MAMBA_HOME = os.getenv("ISOLATE_MAMBA_HOME")

# Mamba accepts the following version specifiers: =, ==, >=, <=, >, <, !=
_POSSIBLE_MAMBA_VERSION_IDENTIFIERS = (
    "=",
    "<",
    ">",
    "!",
)


@dataclass
class MambaEnvironment(BaseEnvironment[Path]):
    BACKEND_NAME: ClassVar[str] = "mamba"

    environment_definition: Dict[str, Any] = field(default_factory=dict)
    python_version: Optional[str] = None

    @classmethod
    def from_config(
        cls,
        config: Dict[str, Any],
        settings: IsolateSettings = DEFAULT_SETTINGS,
    ) -> BaseEnvironment:
        processing_config = copy.deepcopy(config)
        processing_config.setdefault("python_version", active_python())

        if "env_dict" in processing_config:
            definition = processing_config.pop("env_dict")
        elif "env_yml_str" in processing_config:
            yaml = optional_import("yaml")

            definition = yaml.safe_load(processing_config.pop("env_yml_str"))
        elif "packages" in processing_config:
            definition = {
                "dependencies": processing_config.pop("packages"),
            }
        else:
            raise ValueError(
                "Either 'env_dict', 'env_yml_str' or 'packages' must be specified"
            )

        dependencies = definition.setdefault("dependencies", [])
        if _depends_on(dependencies, "python"):
            raise ValueError(
                "Python version can not be specified by the environment but rather ",
                " it needs to be passed as `python_version` option to the environment.",
            )

        dependencies.append(f"python={processing_config['python_version']}")

        # Extend pip dependencies and channels if they are specified.
        if "pip" in processing_config:
            if not _depends_on(dependencies, "pip"):
                dependencies.append("pip")

            try:
                dependency_group = next(
                    dependency
                    for dependency in dependencies
                    if isinstance(dependency, dict) and "pip" in dependency
                )
            except StopIteration:
                dependency_group = {"pip": []}
                dependencies.append(dependency_group)

            dependency_group["pip"].extend(processing_config.pop("pip"))

        if "channels" in processing_config:
            definition.setdefault("channels", [])
            definition["channels"].extend(processing_config.pop("channels"))

        environment = cls(
            environment_definition=definition,
            **processing_config,
        )
        environment.apply_settings(settings)
        return environment

    @property
    def key(self) -> str:
        return sha256_digest_of(repr(self.environment_definition))

    def create(self, *, force: bool = False) -> Path:
        env_path = self.settings.cache_dir_for(self)
        with self.settings.cache_lock_for(env_path):
            if env_path.exists() and not force:
                return env_path

            self.log(f"Creating the environment at '{env_path}'")
            with tempfile.NamedTemporaryFile(mode="w", suffix=".yml") as tf:
                yaml = optional_import("yaml")
                yaml.dump(self.environment_definition, tf)
                tf.flush()

                try:
                    self._run_mamba(
                        "env", "create", "--yes", "--prefix", env_path, "-f", tf.name
                    )
                except subprocess.SubprocessError as exc:
                    raise EnvironmentCreationError(
                        f"Failure during 'mamba create': {exc}"
                    )

            self.log(f"New environment cached at '{env_path}'")
            return env_path

    def destroy(self, connection_key: Path) -> None:
        with self.settings.cache_lock_for(connection_key):
            # It might be destroyed already (when we are awaiting
            # for the lock to be released).
            if not connection_key.exists():
                return

            self._run_mamba(
                "remove",
                "--yes",
                "--all",
                "--prefix",
                connection_key,
            )

            # Unlike conda, mamba leaves a metafile inside the cache_dir, so we have to
            # manually remove it.
            os.system(f"rm -rf {self.settings.cache_dir_for(self)}")

    def _run_mamba(self, *args: Any) -> None:
        mamba_executable = _get_mamba_executable()
        with logged_io(partial(self.log, level=LogLevel.INFO)) as (stdout, stderr):
            subprocess.check_call(
                [mamba_executable, *args],
                stdout=stdout,
                stderr=stderr,
            )

    def exists(self) -> bool:
        path = self.settings.cache_dir_for(self)
        return path.exists()

    def open_connection(self, connection_key: Path) -> PythonIPC:
        return PythonIPC(self, connection_key)


@functools.lru_cache(1)
def _get_mamba_executable() -> Path:
    for path in [_ISOLATE_MAMBA_HOME, None]:
        mamba_path = shutil.which(_MAMBA_COMMAND, path=path)
        if mamba_path is not None:
            return Path(mamba_path)
    else:
        # TODO: we should probably show some instructions on how you
        # can install mamba here.
        raise FileNotFoundError(
            "Could not find mamba executable. If mamba executable is not available by default, please point isolate "
            " to the path where mamba binary is available 'ISOLATE_MAMBA_HOME'."
        )


def _depends_on(
    dependencies: List[Union[str, Dict[str, List[str]]]],
    package_name: str,
) -> bool:
    for dependency in dependencies:
        if isinstance(dependency, dict):
            # It is a dependency group like pip: [...]
            continue

        # Get rid of all whitespace characters (python = 3.8 becomes python=3.8)
        package = dependency.replace(" ", "")
        if not package.startswith(package_name):
            continue

        # Ensure that the package name matches perfectly and not only
        # at the prefix level. Examples:
        #  - python # OK
        #  - python=3.8 # OK
        #  - python>=3.8 # OK
        #  - python-user-toolkit # NOT OK
        #  - pythonhelp!=1.0 # NOT OK
        suffix = package[len(package_name) :]
        if suffix and suffix[0] not in _POSSIBLE_MAMBA_VERSION_IDENTIFIERS:
            continue

        return True
    else:
        return False
