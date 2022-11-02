from dataclasses import dataclass
from typing import Any

import importlib_metadata
import pytest

from isolate._interactive import (
    BoxedEnvironment,
    Environment,
    LocalBox,
    RemoteBox,
)

cp_version = importlib_metadata.version("cloudpickle")
dill_version = importlib_metadata.version("dill")


@pytest.mark.parametrize(
    "kind, params, serialization_backend, expected",
    [
        (
            "virtualenv",
            {"requirements": []},
            "pickle",
            "virtualenv(requirements=[])",
        ),
        (
            "conda",
            {"packages": []},
            "cloudpickle",
            f"conda(packages=['cloudpickle={cp_version}'])",
        ),
        (
            "virtualenv",
            {"requirements": ["pandas"]},
            "pickle",
            "virtualenv(requirements=['pandas'])",
        ),
        (
            "conda",
            {"packages": ["pandas"]},
            "pickle",
            "conda(packages=['pandas'])",
        ),
        (
            "virtualenv",
            {"requirements": ["pyjokes==0.5.0"]},
            "cloudpickle",
            f"virtualenv(requirements=['pyjokes==0.5.0', 'cloudpickle=={cp_version}'])",
        ),
        (
            "conda",
            {"packages": ["pyjokes=0.5.0"]},
            "dill",
            f"conda(packages=['pyjokes=0.5.0', 'dill={dill_version}'])",
        ),
    ],
)
def test_builder(kind, params, serialization_backend, expected, monkeypatch):
    monkeypatch.setattr(
        "isolate._interactive._decide_default_backend", lambda: serialization_backend
    )

    builder = Environment(kind, **params)
    assert repr(builder) == expected


@pytest.mark.parametrize(
    "kind, init_params, forwarded_packages, expected",
    [
        (
            "virtualenv",
            {"requirements": ["pandas"]},
            ["pyjokes"],
            "virtualenv(requirements=['pandas', 'pyjokes'])",
        ),
        (
            "conda",
            {"packages": ["pandas"]},
            ["pyjokes"],
            "conda(packages=['pandas', 'pyjokes'])",
        ),
        (
            "virtualenv",
            {"requirements": ["pyjokes==0.5.0"]},
            ["emoji==0.5.0", "pandas", "whatever==3.0.0"],
            "virtualenv(requirements=['pyjokes==0.5.0', 'emoji==0.5.0', 'pandas', 'whatever==3.0.0'])",
        ),
        (
            "conda",
            {"packages": ["pyjokes=0.5.0"]},
            ["emoji=0.5.0", "pandas", "whatever=3.0.0"],
            "conda(packages=['pyjokes=0.5.0', 'emoji=0.5.0', 'pandas', 'whatever=3.0.0'])",
        ),
    ],
)
def test_builder_forwarding(
    kind, init_params, forwarded_packages, expected, monkeypatch
):
    # Use pickle to avoid adding the default backend to the requirements
    monkeypatch.setattr(
        "isolate._interactive._decide_default_backend", lambda: "pickle"
    )

    builder = Environment(kind, **init_params)
    for forwarded_package in forwarded_packages:
        builder << forwarded_package
    assert repr(builder) == expected


@dataclass
class UncachedLocalBox(LocalBox):
    """Prevent caching of test environments when running
    these tests locally."""

    cache_dir: Any

    def wrap_it(self, *args: Any, **kwargs: Any) -> BoxedEnvironment:
        boxed_env = super().wrap_it(*args, **kwargs)
        boxed_env.environment.apply_settings(
            boxed_env.environment.settings.replace(cache_dir=self.cache_dir)
        )
        return boxed_env


def test_local_box(tmp_path):
    builder = Environment("virtualenv")
    builder << "pyjokes==0.5.0"

    environment = builder >> UncachedLocalBox(tmp_path)
    result = environment.run(eval, "__import__('pyjokes').__version__")
    assert result == "0.5.0"


def test_remote_box(isolate_server):
    builder = Environment("virtualenv")
    builder << "pyjokes==0.5.0"

    # Remote box is uncached by default (isolate_server handles it).
    environment = builder >> RemoteBox(isolate_server)
    result = environment.run(eval, "__import__('pyjokes').__version__")
    assert result == "0.5.0"
