from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

import dill

# A mapping of machine label -> host value
_MACHINE_LABELS: Dict[str, str] = {}


@dataclass(init=False)
class Environment:
    kind: str
    configuration: Dict[str, Any]
    collected_param: Set[Any]
    inherits_local: bool = False

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

    def inherit_local(self):
        self.inherits_local = True
        return self

    def dump(self) -> Dict[str, Any]:
        if self.kind == "virtualenv":
            param = "requirements"
            self.collected_param.add(f"dill=={dill.__version__}")
        elif self.kind == "conda":
            param = "packages"
            self.collected_param.add(f"dill={dill.__version__}")
        else:
            raise NotImplementedError(self.kind)

        config = self.configuration.copy()
        config.setdefault(param, []).extend(sorted(self.collected_param))
        return {"kind": self.kind, **config}


@dataclass
class RemoteEnvironment:
    host: str
    environment: Optional[Environment] = None

    def dump(self):
        config = self.environment.dump()
        if self.environment.inherits_local:
            raise ValueError(
                "Inheriting local environment is currently not supported for "
                "remote environments."
            )

        return {
            "kind": "isolate-server",
            "host": self.host,
            "target_environment_kind": config.pop("kind"),
            "target_environment_config": config,
        }


def Machine(label: str) -> RemoteEnvironment:
    return RemoteEnvironment(_MACHINE_LABELS[label])
