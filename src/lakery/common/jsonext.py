from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import TypedDict
from typing import cast
from typing import overload
from uuid import UUID

from lakery.core.storable import Storable
from lakery.core.unpacker import UnpackedValue
from lakery.core.unpacker import UnpackedValueStream

if TYPE_CHECKING:
    from lakery.common.types import JsonType
    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


def dump_json_ext(
    value: Any,
    registry: Registry,
) -> tuple[JsonType, Mapping[str, UnpackedValue | UnpackedValueStream]]:
    """Dump the given value to a JSON-serializable object with extensions."""
    context: JsonExtDumpContext = {
        "path": "",
        "registry": registry,
        "external": {},
    }
    dumped_value = _dump_json_ext(value, context)
    return dumped_value, context["external"]


def _dump_json_ext(
    value: Any,
    context: JsonExtDumpContext,
    default: Callable[[Any], UnpackedValue | None] = lambda x: None,
) -> JsonType:
    path = context["path"]
    match value:
        case str() | int() | float() | bool() | None:
            return value
        case Mapping():
            return {
                k: _dump_json_ext(v, {**context, "path": f"{path}/{k}"}) for k, v in value.items()
            }
        case Sequence():
            return [
                _dump_json_ext(v, {**context, "path": f"{path}/{i}"}) for i, v in enumerate(value)
            ]
        case _:
            return cast(
                "dict",
                _dump_any_json_ext(
                    path,
                    (
                        value
                        if isinstance(value, Storable)
                        else (
                            default(value) or {"value": value, "serializer": None, "storage": None}
                        )
                    ),
                    context,
                ),
            )


def load_json_ext(
    value: JsonType,
    registry: Registry,
    external: Mapping[str, UnpackedValue | UnpackedValueStream],
) -> Any:
    """Load the given JSON-serializable object with extensions to a value."""
    return _load_json_ext(value, {"registry": registry, "external": external})


def _load_json_ext(value: Any, context: JsonExtLoadContext) -> Any:
    match value:
        case str() | int() | float() | bool() | None:
            return value
        case Mapping():
            if "__json_ext__" in value:
                return _load_any_json_ext(cast("_AnyJsonExt", value), context)
            else:
                return {k: _load_json_ext(v, context) for k, v in value.items()}
        case Sequence():
            return [_load_json_ext(v, context) for v in value]
        case _:
            return value


class JsonExtDumpContext(TypedDict):
    """The context for dumping extended JSON data."""

    path: str
    registry: Registry
    external: dict[str, UnpackedValue | UnpackedValueStream]


class JsonExtLoadContext(TypedDict):
    """The context for loading extended JSON data."""

    registry: Registry
    external: Mapping[str, UnpackedValue | UnpackedValueStream]


class _RefJsonExt(TypedDict):
    """A JSON extension for external content references."""

    __json_ext__: Literal["ref"]

    ref: str
    """The reference to the external content."""


class _ContentJsonExt(TypedDict):
    """A JSON extension for external content data."""

    __json_ext__: Literal["content"]

    content_base64: str
    """The base64 encoded data."""
    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    serializer_name: str
    """The name of the serializer used to serialize the data."""


class _ObjectConfigJsonExt(TypedDict):
    """The configuration of the storable object."""

    class_id: str
    """The class ID of the storable object."""
    unpacker_name: str
    """The name of the unpacker used to unpack the storable object."""


class _ObjectJsonExt(TypedDict):
    """A JSON extension for external storable object."""

    __json_ext__: Literal["object"]

    config: _ObjectConfigJsonExt
    """The configuration of the storable object."""
    contents: dict[str, _ContentJsonExt | _RefJsonExt]
    """Content dumped by the model."""


_AnyJsonExt = _RefJsonExt | _ContentJsonExt | _ObjectJsonExt
"""The JSON extension for external content references."""


@overload
def _dump_any_json_ext(
    key: str,
    data: UnpackedValue,
    context: JsonExtDumpContext,
) -> _ContentJsonExt | _RefJsonExt: ...


@overload
def _dump_any_json_ext(
    key: str,
    data: Storable,
    context: JsonExtDumpContext,
) -> _ObjectJsonExt: ...


def _dump_any_json_ext(
    key: str,
    data: Storable | UnpackedValue | UnpackedValueStream,
    context: JsonExtDumpContext,
) -> _AnyJsonExt:
    """Dump the given value to a JSON extension."""
    match data:
        case Storable():
            return _dump_json_model_ext(data, context)
        case {"value": value, "serializer": serializer, "storage": storage}:
            if storage is None:
                return _dump_json_content_ext(value, serializer, context)
            else:
                return _dump_json_ref_ext(key, value, serializer, storage, context, stream=False)
        case {"value_stream": value_stream, "serializer": serializer, "storage": storage}:
            if storage is None:
                storage = context["registry"].get_default_storage()
            return _dump_json_ref_ext(key, value_stream, serializer, storage, context, stream=True)
        case _:
            msg = f"Unsupported data type for JSON extension: {data}"
            raise TypeError(msg)


def _dump_json_content_ext(
    value: Any, serializer: Serializer | None, context: JsonExtDumpContext
) -> _ContentJsonExt:
    """Dump the given value to a JSON extension with embedded content."""
    serializer = serializer or context["registry"].get_serializer_by_type(type(value))
    content = serializer.serialize_data(value)
    return {
        "__json_ext__": "content",
        "content_base64": b64encode(content["data"]).decode("ascii"),
        "content_encoding": content["content_encoding"],
        "content_type": content["content_type"],
        "serializer_name": serializer.name,
    }


def _dump_json_model_ext(value: Storable, context: JsonExtDumpContext) -> _ObjectJsonExt:
    """Dump the given value to a JSON extension with a storage model."""
    cls = type(value)
    cfg = cls.get_storable_config()
    context["registry"].has_storable(cls)
    return {
        "__json_ext__": "object",
        "config": {
            "class_id": cfg.class_id.hex,
            "unpacker_name": cfg.unpacker.name,
        },
        "contents": {
            k: _dump_any_json_ext(k, v, context)
            for k, v in cfg.unpacker.unpack_object(value, context["registry"]).items()
        },
    }


@overload
def _dump_json_ref_ext(
    key: str,
    value: Any,
    serializer: Serializer | None,
    storage: Storage,
    context: JsonExtDumpContext,
    *,
    stream: Literal[False],
) -> _RefJsonExt: ...


@overload
def _dump_json_ref_ext(
    key: str,
    value: Any,
    serializer: StreamSerializer | None,
    storage: Storage,
    context: JsonExtDumpContext,
    *,
    stream: Literal[True],
) -> _RefJsonExt: ...


def _dump_json_ref_ext(
    key: str,
    value: Any,
    serializer: Serializer | StreamSerializer | None,
    storage: Storage,
    context: JsonExtDumpContext,
    *,
    stream: bool,
) -> _RefJsonExt:
    """Dump the given value to a JSON extension with a reference."""
    path = f"{context['path']}/{key}"

    if stream:
        context["external"][path] = UnpackedValueStream(
            {
                "value_stream": value,
                "serializer": cast("StreamSerializer", serializer),
                "storage": storage,
            }
        )
    else:
        context["external"][path] = UnpackedValue(
            {
                "value": value,
                "serializer": cast("Serializer", serializer),
                "storage": storage,
            }
        )

    return {"__json_ext__": "ref", "ref": path}


def _load_any_json_ext(value: _AnyJsonExt, context: JsonExtLoadContext) -> Any:
    """Load a value from a JSON extension."""
    match value:
        case {"__json_ext__": ext}:
            pass
        case _:
            msg = f"Expected JSON extension, got: {value}"
            raise ValueError(msg)
    try:
        return _load_func_by_ext[ext](value, context)
    except KeyError:
        msg = f"Unknown JSON extension: {ext}"
        raise ValueError(msg) from None


def _load_json_content_ext(json_ext: _ContentJsonExt, context: JsonExtLoadContext) -> Any:
    """Load a value from a JSON extension with embedded content."""
    serializer = context["registry"].get_serializer(json_ext["serializer_name"])
    return serializer.deserialize_data(
        {
            "data": b64decode(json_ext["content_base64"].encode("ascii")),
            "content_encoding": json_ext["content_encoding"],
            "content_type": json_ext["content_type"],
        }
    )


def _load_json_ref_ext(value: _RefJsonExt, context: JsonExtLoadContext) -> Any:
    """Load a value from a JSON extension with a reference."""
    match ref := context["external"][value["ref"]]:
        case {"value": value}:
            return value
        case {"value_stream": value_stream}:
            return value_stream
        case _:
            msg = f"Expected a reference to a value or value stream, got: {ref}"
            raise ValueError(msg)


def _load_json_model_ext(value: _ObjectJsonExt, context: JsonExtLoadContext) -> Storable:
    """Load a value from a JSON extension with a storage model."""
    registry = context["registry"]
    cfg = value["config"]
    cls = registry.get_storable(UUID(cfg["class_id"]))
    unpacker = registry.get_unpacker(cfg["unpacker_name"])
    contents = {k: _load_any_json_ext(v, context) for k, v in value["contents"].items()}
    return unpacker.repack_object(cls, contents, registry)


_load_func_by_ext = {
    "content": _load_json_content_ext,
    "ref": _load_json_ref_ext,
    "model": _load_json_model_ext,
}
