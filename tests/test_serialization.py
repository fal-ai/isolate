from functools import partial

import pytest
from isolate.connections.common import (
    SerializationError,
    load_serialized_object,
    serialize_object,
)


@pytest.mark.parametrize(
    "method",
    [
        "pickle",
        "dill",
        "cloudpickle",
    ],
)
def test_serialize_object(method):
    func = partial(eval, "2 + 2")
    serialized = serialize_object(method, func)
    deserialized = load_serialized_object(method, serialized)
    assert deserialized() == 4


def test_deserialize_exception():
    serialized = serialize_object("pickle", ValueError("some error"))
    regular_obj = load_serialized_object("pickle", serialized)
    assert isinstance(regular_obj, ValueError)
    assert regular_obj.args == ("some error",)


def test_deserialize_raised_exception():
    serialized = serialize_object("pickle", ValueError("some error"))
    with pytest.raises(ValueError) as exc_info:
        load_serialized_object("pickle", serialized, was_it_raised=True)
    assert exc_info.value.args == ("some error",)


def error_while_serializing():
    anon = lambda: 2 + 2  # anonymous functions are not  # noqa: E731
    # serializable by pickle
    with pytest.raises(SerializationError) as exc_info:
        serialize_object("pickle", anon)

    assert exc_info.match("Error while serializing the given object")

    dill_serialized_lambda = serialize_object("dill", anon)

    with pytest.raises(SerializationError) as exc_info:
        load_serialized_object("pickle", dill_serialized_lambda)

    assert exc_info.match("Error while deserializing the given object")


def error_while_loading_backend():
    with pytest.raises(SerializationError) as exc_info:
        serialize_object("$$$", 1)

    assert exc_info.match("Error while preparing the serialization backend")

    with pytest.raises(SerializationError) as exc_info:
        load_serialized_object("$$$", b"1")

    assert exc_info.match("Error while preparing the serialization backend")
