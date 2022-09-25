from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, NewType

from platformdirs import user_cache_dir

if TYPE_CHECKING:
    from isolate.backends import BaseEnvironment


@dataclass(frozen=True)
class _Context:
    _base_cache_dir: Path = field(repr=False)
    _serialization_backend: str = "pickle"

    def get_cache_dir(self, backend: BaseEnvironment) -> Path:
        backend_name = backend.BACKEND_NAME
        assert backend_name is not None
        return self._base_cache_dir / backend_name

    @property
    def serialization_backend_name(self) -> str:
        return self._serialization_backend


# We don't want to expose the context API just yet, but still want people
# to properly annotate it.
ContextType = NewType("ContextType", _Context)
GLOBAL_CONTEXT = ContextType(
    _Context(
        _base_cache_dir=Path(
            user_cache_dir("isolate", "isolate"),
        )
    )
)
