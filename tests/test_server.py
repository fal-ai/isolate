import importlib
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import partial
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Tuple

import pytest
from flask.testing import FlaskClient

from isolate.server import app
from isolate.server._server import ENV_STORE, RUN_STORE

REPO_DIR = Path(__file__).parent.parent
assert (
    REPO_DIR.exists() and REPO_DIR.name == "isolate"
), "This test should have access to isolate as an installable package."


@dataclass
class APIWrapper:
    client: FlaskClient

    def create(self, kind: str, **configuration: Any) -> str:
        response = self.client.post(
            "/environments/create",
            json={
                "kind": kind,
                "configuration": configuration,
            },
        )

        data = response.json
        assert data["status"] == "success"
        return data["token"]

    def run(
        self,
        environment_token: str,
        function: Any,
        serialization_method: str,
    ) -> str:
        serializer = importlib.import_module(serialization_method)
        response = self.client.post(
            "/environments/runs",
            query_string={
                "environment_token": environment_token,
                "serialization_backend": serialization_method,
            },
            data=serializer.dumps(function),
        )

        data = response.json
        assert data["status"] == "success"
        return data["token"]

    def status(self, run_token: str, logs_start: int = 0) -> Dict[str, Any]:
        response = self.client.get(
            f"/environments/runs/{run_token}/status",
            query_string={
                "logs_start": logs_start,
            },
        )

        data = response.json
        assert data["status"] == "success"
        return data

    def iter_logs_till_done(
        self,
        run_token: str,
        # Maximum amount of time to wait for the run to finish
        max_time: int = 60,
        # How often to check the status
        per_poll_interval: float = 0.1,
    ) -> Iterator[Dict[str, Any]]:
        logs_start = 0
        for _ in range(round(max_time / per_poll_interval)):
            time.sleep(per_poll_interval)

            status = self.status(run_token, logs_start)
            logs_start += len(status["logs"])
            yield from status["logs"]
            if status["is_done"]:
                break
        else:
            raise TimeoutError(f"The run did not finish in {max_time}!")


def reset_state():
    ENV_STORE.clear()
    RUN_STORE.clear()


def split_logs(logs: Iterable[Dict[str, str]]) -> Tuple[List[Tuple[str, str]], ...]:
    """Split the given iterable of logs into three categories (by their source):
        - builder
        - bridge
        - user

    Each category is a list of tuples of (level, message).
    """
    categories = defaultdict(list)
    for log in logs:
        categories[log["source"]].append((log["level"], log["message"]))

    return (
        categories["builder"],
        categories["bridge"],
        categories["user"],
    )


def inherit_from_local(monkeypatch: Any, value: bool = True) -> None:
    """Enables the inherit from local mode for the isolate server."""
    monkeypatch.setattr("isolate.server._runs.INHERIT_FROM_LOCAL", value)


@pytest.fixture
def api_wrapper():
    reset_state()
    with app.test_client() as client:
        yield APIWrapper(client)
    reset_state()


@pytest.mark.parametrize("inherit_local", [True, False])
def test_isolate_server(
    api_wrapper: APIWrapper,
    inherit_local: bool,
    monkeypatch: Any,
) -> None:
    inherit_from_local(monkeypatch, inherit_local)

    requirements = ["pyjokes==0.6.0"]
    if not inherit_local:
        # The agent process needs dill (and isolate) to actually
        # deserialize the given function, so they need to be installed
        # when we are not inheriting the local environment.
        requirements.append("dill==0.3.5.1")

        # TODO: this can probably install the local version of isolate
        # you have when you are running the tests.
        requirements.append(str(REPO_DIR))

    environment_token = api_wrapper.create("virtualenv", requirements=requirements)
    run_token = api_wrapper.run(
        environment_token,
        partial(
            exec,
            "import pyjokes; print('version:', pyjokes.__version__)",
        ),
        serialization_method="dill",
    )
    logs = list(api_wrapper.iter_logs_till_done(run_token))
    _, _, user_logs = split_logs(logs)
    assert user_logs == [
        ("stdout", "version: 0.6.0"),
    ]


def test_environment_building_error(api_wrapper: APIWrapper, monkeypatch: Any) -> None:
    inherit_from_local(monkeypatch)

    # $$$$ as a package can't exist on PyPI since PEP 508 explicitly defines
    # what is considered to be a legit package name.
    #
    # https://peps.python.org/pep-0508/#names

    # Since we are not building environments until they ran, this should succeed.
    environment_token = api_wrapper.create("virtualenv", requirements=["$$$$"])
    run_token = api_wrapper.run(
        environment_token,
        partial(
            exec,
            "print(1 + 1)",
        ),
        serialization_method="pickle",
    )

    logs = list(api_wrapper.iter_logs_till_done(run_token))
    builder_logs, _, user_logs = split_logs(logs)
    assert builder_logs[-1] == (
        "error",
        "Failed to create the environment. Aborting the run.",
    )
    assert not user_logs


def test_isolate_server_auth_error(
    api_wrapper: APIWrapper,
    monkeypatch: Any,
) -> None:
    inherit_from_local(monkeypatch)

    # Activates authentication requirement
    app.config['USER_NAME'] = "testuser"

    requirements = ["pyjokes==0.6.0"]

    response = api_wrapper.client.post(
        "/environments/create",
        json={
            "kind": "virtualenv",
            "configuration": {'requirements': requirements}
        },
    )

    assert response.status_code == 401

    data = response.json

    assert data["status"] == "error"
    assert data["message"] == "A valid token is missing"


def test_isolate_server_auth_invalid_token(
    api_wrapper: APIWrapper,
    monkeypatch: Any,
) -> None:
    inherit_from_local(monkeypatch)

    # Activates authentication requirement
    app.config['USER_NAME'] = "testuser"

    requirements = ["pyjokes==0.6.0"]

    response = api_wrapper.client.post(
        "/environments/create",
        headers={
            "x-access-token": "invalid token"
        },
        json={
            "kind": "virtualenv",
            "configuration": {'requirements': requirements}
        },
    )

    assert response.status_code == 401

    data = response.json

    assert data["status"] == "error"
    assert data["message"] == "Invalid token"


def test_isolate_server_auth(
    api_wrapper: APIWrapper,
    monkeypatch: Any,
) -> None:
    from isolate.server._auth import create_auth_token
    inherit_from_local(monkeypatch)

    # Activates authentication requirement
    app.config["USER_NAME"] = "testuser"
    app.config["SECRET_KEY"] = "testkey"

    token = create_auth_token(app.config['USER_NAME'], app.config['SECRET_KEY'])

    requirements = ["pyjokes==0.6.0"]

    response = api_wrapper.client.post(
        "/environments/create",
        headers={
            "x-access-token": token
        },
        json={
            "kind": "virtualenv",
            "configuration": {'requirements': requirements}
        },
    )

    assert response.status_code == 200
    assert response.json["status"] == 'success'

