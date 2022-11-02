import copy
import inspect
import threading
from collections import ChainMap
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from typing import Any, Dict, Optional, Set

import dill

import isolate
from isolate.backends import BaseEnvironment
from isolate.backends.settings import IsolateSettings
from isolate.logs import Log


class _BaseDefinition:
    def dump(self) -> Dict[str, Any]:
        """Serialize the environment definition to a form where
        isolate can process it natively."""
        raise NotImplementedError

    def prepare(self) -> BaseEnvironment:
        """Prepare the isolate environment from the definition and return it."""
        settings = IsolateSettings(serialization_method="dill")
        return isolate.prepare_environment(**self.dump(), context=settings)

    def run(self, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Run the given function in the environment defined by this definition."""
        with self.prepare().connect() as connection:
            return connection.run(partial(func, *args, **kwargs))


def add_default_deps(config, collected_params=None):
    collected_params = collected_params or []
    if config["kind"] == "virtualenv":
        param = "requirements"
        extra_dep = f"dill=={dill.__version__}"
    elif config["kind"] == "conda":
        param = "packages"
        extra_dep = f"dill={dill.__version__}"
    else:
        raise NotImplementedError(self.kind)

    config.setdefault(param, []).extend(sorted(collected_params + [extra_dep]))
    return config


@dataclass(init=False)
class Environment(_BaseDefinition):
    kind: str
    configuration: Dict[str, Any]
    collected_param: Set[Any]

    def __init__(self, kind: str, **configuration: Any) -> None:
        self.kind = kind
        self.configuration = configuration
        self.collected_param = set()

    def __lshift__(self, right: Any) -> Any:
        if not isinstance(right, str):
            return NotImplemented

        self.collected_param.add(right)
        return self

    def __rshift__(self, right: Any) -> Any:
        if not isinstance(right, RemoteEnvironment):
            return NotImplemented

        right.environment = self
        return right

    def dump(self) -> Dict[str, Any]:
        config = {"kind": self.kind, **self.configuration}
        return add_default_deps(config, self.collected_param.copy())


@dataclass
class RemoteEnvironment(_BaseDefinition):
    host: str
    environment: Optional[Environment] = None
    max_workers: int = 1

    def dump(self):
        if self.environment is None:
            for frame in inspect.stack():
                namespace = ChainMap(frame[0].f_globals, frame[0].f_locals)
                if "_global_isolate_env" in namespace:
                    config = add_default_deps(
                        copy.deepcopy(namespace["_global_isolate_env"]), []
                    )
                    break
            else:
                raise RuntimeError(
                    "Machines must have an environment; try using `env >> machine`"
                )
        else:
            config = self.environment.dump()

        return {
            "kind": "isolate-server",
            "host": self.host,
            "target_environment_kind": config.pop("kind"),
            "target_environment_config": config,
        }

    def __mul__(self, num_workers: int) -> "RemoteEnvironment":
        return RemoteEnvironment(
            self.host,
            self.environment,
            self.max_workers * num_workers,
        )

    def map(self, func: Any, args: Any) -> Any:
        def per_thread_log(log: Log) -> None:
            print(threading.get_ident(), log)

        # We only create threads to spawn concurrent requests, not to actually
        # run the functions.
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            environment = self.prepare()
            environment.apply_settings(IsolateSettings(log_hook=per_thread_log))
            with environment.connect() as connection:
                futures = [
                    executor.submit(connection.run, partial(func, arg)) for arg in args
                ]
                for future in as_completed(futures):
                    yield future.result()


def Machine(label: str, host: str) -> RemoteEnvironment:
    return RemoteEnvironment(host)
