import functools
from typing import Any, Callable, Dict, Tuple, TypeVar

from flask import jsonify
from marshmallow import ValidationError

T = TypeVar("T")


def error(code: int = 400, **kwargs: Any) -> Tuple[Any, int]:
    return jsonify({"status": "error", **kwargs}), code


def success(code: int = 200, **kwargs: Any) -> Tuple[Any, int]:
    return jsonify({"status": "success", **kwargs}), code


def load_token(store: Dict[str, T], token: str) -> T:
    if token not in store:
        raise ValidationError(f"Invalid token: '{token}'")
    return store[token]


def wrap_validation_errors(func: Callable[..., T]) -> Callable[..., T]:
    """Convert any ValidationError happened during the call
    to the wrapped function into a JSON error response."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except ValidationError as err:
            return error(message=str(err))

    return wrapper
