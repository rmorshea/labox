from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import LiteralString
from typing import TypedDict
from typing import cast
from typing import overload
from uuid import UUID

from lakery.core.decomposer import AnyModeledValue
from lakery.core.decomposer import BaseStorageModel
from lakery.core.decomposer import StorageModelConfigDict

if TYPE_CHECKING:
    from lakery.core.decomposer import UnpackedValue
    from lakery.core.registry import RegistryCollection
    from lakery.core.serializer import Serializer
    from lakery.core.storage import Storage

JsonType = (
    int
    | str
    | float
    | bool
    | dict[str, "JsonType"]
    | list["JsonType"]
    | tuple["JsonType", ...]
    | None
)
"""A type alias for JSON data."""
JsonStreamType = dict[str, JsonType] | list[JsonType] | tuple[JsonType, ...]
"""A type alias for a a value in a stream of JSON data."""


class RefJsonExt(TypedDict):
    """A JSON extension for external content references."""

    __json_ext__: Literal["ref"]

    ref: str
    """The reference to the external content."""


class ContentJsonExt(TypedDict):
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


class ModelJsonExt(TypedDict):
    """A JSON extension for external storage models."""

    __json_ext__: Literal["model"]

    config: StorageModelConfigDict
    """The configuration of the storage model."""
    contents: dict[str, ContentJsonExt | RefJsonExt]
    """Content dumped by the model."""


AnyJsonExt = RefJsonExt | ContentJsonExt | ModelJsonExt
"""The JSON extension for external content references."""


class JsonExtDumpContext(TypedDict):
    """The context for dumping extended JSON data."""

    path: str
    registries: RegistryCollection
    external: dict[str, UnpackedValue]


class JsonExtLoadContext(TypedDict):
    """The context for loading extended JSON data."""

    registries: RegistryCollection
    external: dict[str, UnpackedValue]


def dump_json_ext(
    value: Any,
    context: JsonExtDumpContext,
    default: Callable[[Any], UnpackedValue | None] = lambda x: None,
) -> JsonType:
    """Dump the given value to JSON with extensions."""
    path = context["path"]
    match value:
        case str() | int() | float() | bool() | None:
            return value
        case Mapping():
            return {
                k: dump_json_ext(v, {**context, "path": f"{path}/{k}"}) for k, v in value.items()
            }
        case Sequence():
            return [
                dump_json_ext(v, {**context, "path": f"{path}/{i}"}) for i, v in enumerate(value)
            ]
        case _:
            return cast(
                "dict",
                dump_any_json_ext(
                    path,
                    (
                        value
                        if isinstance(value, BaseStorageModel)
                        else (
                            default(value) or {"value": value, "serializer": None, "storage": None}
                        )
                    ),
                    context,
                ),
            )


@overload
def dump_any_json_ext(
    key: str,
    data: UnpackedValue,
    context: JsonExtDumpContext,
) -> ContentJsonExt | RefJsonExt: ...


@overload
def dump_any_json_ext(
    key: str,
    data: BaseStorageModel,
    context: JsonExtDumpContext,
) -> ModelJsonExt: ...


def dump_any_json_ext(
    key: str,
    data: BaseStorageModel | UnpackedValue,
    context: JsonExtDumpContext,
) -> AnyJsonExt:
    """Dump the given value to a JSON extension."""
    if isinstance(data, BaseStorageModel):
        return dump_json_model_ext(data, context)
    return (
        dump_json_ref_ext(key, data["value"], data["serializer"], storage, context)
        if (storage := data["storage"]) is not None
        else dump_json_content_ext(data["value"], data["serializer"], context)
    )


def dump_json_content_ext(
    value: Any, serializer: Serializer | None, context: JsonExtDumpContext
) -> ContentJsonExt:
    """Dump the given value to a JSON extension with embedded content."""
    serializer = serializer or context["registries"].serializers.infer_from_value_type(type(value))
    content = serializer.deserialize_data(value)
    return {
        "__json_ext__": "content",
        "content_base64": b64encode(content["data"]).decode("ascii"),
        "content_encoding": content["content_encoding"],
        "content_type": content["content_type"],
        "serializer_name": serializer.name,
    }


def dump_json_model_ext(value: BaseStorageModel, context: JsonExtDumpContext) -> ModelJsonExt:
    """Dump the given value to a JSON extension with a storage model."""
    cls = type(value)
    cfg = cls.storage_model_config()
    context["registries"].models.check_registered(cls)
    return {
        "__json_ext__": "model",
        "config": {
            "id": cast("LiteralString", cfg.id.hex),
            "version": cfg.version,
        },
        "contents": {
            k: dump_any_json_ext(k, _check_is_not_stream_content(v), context)
            for k, v in value.storage_model_dump(context["registries"]).items()
        },
    }


def _check_is_not_stream_content(cont: AnyModeledValue) -> UnpackedValue:
    if "value_stream" in cont:
        msg = f"Stream content not supported: {cont}"
        raise ValueError(msg)
    return cont


def dump_json_ref_ext(
    key: str,
    value: Any,
    serializer: Serializer | None,
    storage: Storage,
    context: JsonExtDumpContext,
) -> RefJsonExt:
    """Dump the given value to a JSON extension with a reference."""
    path = f"{context['path']}/{key}"
    context["external"][path] = {
        "value": value,
        "serializer": serializer,
        "storage": storage,
    }
    return {"__json_ext__": "ref", "ref": path}


def load_json_ext(value: Any, context: JsonExtLoadContext) -> Any:
    """Load a value from JSON with extensions."""
    match value:
        case str() | int() | float() | bool() | None:
            return value
        case Mapping():
            if "__json_ext__" in value:
                return load_any_json_ext(cast("AnyJsonExt", value), context)
            else:
                return {k: load_json_ext(v, context) for k, v in value.items()}
        case Sequence():
            return [load_json_ext(v, context) for v in value]
        case _:
            return value


def load_any_json_ext(value: AnyJsonExt, context: JsonExtLoadContext) -> Any:
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


def load_json_content_ext(json_ext: ContentJsonExt, context: JsonExtLoadContext) -> Any:
    """Load a value from a JSON extension with embedded content."""
    serializer = context["registries"].serializers[json_ext["serializer_name"]]
    return serializer.load_data(
        {
            "data": b64decode(json_ext["content_base64"].encode("ascii")),
            "content_encoding": json_ext["content_encoding"],
            "content_type": json_ext["content_type"],
        }
    )


def load_json_ref_ext(value: RefJsonExt, context: JsonExtLoadContext) -> Any:
    """Load a value from a JSON extension with a reference."""
    return context["external"][value["ref"]]["value"]


def load_json_model_ext(value: ModelJsonExt, context: JsonExtLoadContext) -> BaseStorageModel:
    """Load a value from a JSON extension with a storage model."""
    cfg = value["config"]
    cls = context["registries"].models[UUID(cfg["id"])]
    contents = {k: load_any_json_ext(v, context) for k, v in value["contents"].items()}
    return cls.storage_model_load(contents, cfg["version"], context["registries"])


_load_func_by_ext = {
    "content": load_json_content_ext,
    "ref": load_json_ref_ext,
    "model": load_json_model_ext,
}
