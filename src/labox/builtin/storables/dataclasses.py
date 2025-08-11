from __future__ import annotations

from collections.abc import AsyncIterator
from collections.abc import Mapping
from dataclasses import KW_ONLY
from dataclasses import fields
from dataclasses import is_dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import TypedDict
from typing import TypeVar
from typing import Unpack
from uuid import UUID

from labox._internal._simplify import LaboxRefDict
from labox._internal._simplify import dump_content_dict
from labox._internal._simplify import load_content_dict
from labox._internal._utils import full_class_name
from labox.core.serializer import Serializer
from labox.core.serializer import StreamSerializer
from labox.core.storable import Storable
from labox.core.storable import StorableConfigDict
from labox.core.storage import Storage
from labox.core.unpacker import AnyUnpackedValue
from labox.core.unpacker import Unpacker

if TYPE_CHECKING:
    from labox.core.registry import Registry

T = TypeVar("T", default=Any)
__all__ = ("StorableDataclass",)


class StorableDataclassUnpacker(Unpacker["StorableDataclass"]):
    """Unpacker for [`StorableDataclass`][labox.builtin.storables.dataclasses.StorableDataclass]."""

    name = "labox.builtin.dataclass@v1"

    def unpack_object(
        self,
        obj: StorableDataclass,
        registry: Registry,
    ) -> Mapping[str, AnyUnpackedValue]:
        if not is_dataclass(obj):
            msg = f"Expected a storable dataclass, got {type(obj)}"
            raise TypeError(msg)

        external: dict[str, AnyUnpackedValue] = {}
        body = _dump_storable_dataclass(obj, registry=registry, external=external, path=("/ref",))
        return {
            "body": {
                "serializer": obj.storable_dataclass_serializer(registry),
                "storage": obj.storable_dataclass_storage(registry),
                "value": body,
            },
            **external,
        }

    def repack_object(
        self,
        cls: type[StorableDataclass],
        contents: Mapping[str, AnyUnpackedValue],
        registry: Registry,
    ) -> StorableDataclass:
        match contents:
            case {"body": {"value": data_value}, **external}:
                pass
            case {"body": body}:
                msg = f"No 'value' in body: {body}"
                raise ValueError(msg)
            case _:
                msg = "No 'body' in contents."
                raise ValueError(msg)
        obj = _load_storable_dataclass(data_value, registry, external=external)
        if not isinstance(obj, cls):
            msg = f"Expected {cls.__name__}, got {type(obj).__name__}"
            raise TypeError(msg)
        return obj


if TYPE_CHECKING:
    from dataclasses import DataclassInstance  # type: ignore

    _FakeBase = DataclassInstance
else:
    _FakeBase = object


class Config(StorableConfigDict, total=False):
    extra_fields: Literal["ignore", "forbid"]


class StorableDataclass(Storable, _FakeBase, unpacker=StorableDataclassUnpacker()):
    """A base for user-defined storable dataclasses."""

    _storable_class_info: ClassVar[_StorableDataclassInfo] = {
        "extra_fields": "forbid",
    }

    def __init_subclass__(cls, **kwargs: Unpack[Config]):
        # Ensure this is always kw_only
        cls.__annotations__ = {"_": KW_ONLY, **cls.__annotations__}
        cls._storable_class_info = {
            "extra_fields": kwargs.get(
                "extra_fields",
                cls._storable_class_info["extra_fields"],
            ),
        }
        super().__init_subclass__(**kwargs)

    def storable_dataclass_serializer(self, registry: Registry) -> Serializer:
        """Return the serializer for the body of this storable class."""
        return registry.get_serializer_by_content_type("application/json")

    def storable_dataclass_storage(self, registry: Registry) -> Storage:
        """Return the storage for the body of this storable class."""
        return registry.get_default_storage()


class _StorableDataclassInfo(TypedDict):
    """Metadata for a storable class."""

    extra_fields: Literal["ignore", "forbid"]


def _dump_storable_dataclass(
    obj: StorableDataclass,
    *,
    registry: Registry,
    external: dict[str, AnyUnpackedValue],
    path: tuple[str, ...] = (),
) -> _LaboxStorableDataclassDict:
    field_dict: dict[str, Any] = {}
    for f in fields(obj):
        if not f.init:
            continue

        f_name = f.name
        f_value = getattr(obj, f_name)

        if isinstance(f_value, StorableDataclass):
            field_dict[f_name] = _dump_storable_dataclass(
                f_value,
                registry=registry,
                external=external,
                path=(*path, f_name),
            )
            continue

        serializer = (
            registry.get_serializer(serializer_type.name)
            if isinstance(serializer_type := f.metadata.get("serializer"), type)
            and issubclass(serializer_type, Serializer)
            else None
        )
        stream_serializer = (
            registry.get_stream_serializer(stream_serializer_type.name)
            if isinstance(stream_serializer_type := f.metadata.get("stream_serializer"), type)
            and issubclass(stream_serializer_type, StreamSerializer)
            else None
        )
        storage = (
            registry.get_storage(storage_type.name)
            if isinstance(storage_type := f.metadata.get("storage"), type)
            and issubclass(storage_type, Storage)
            else None
        )

        if storage is not None:
            path_str = "/".join((*path, f_name))
            if serializer is not None:
                external[path_str] = {
                    "value": f_value,
                    "serializer": serializer,
                    "storage": storage,
                }
            elif stream_serializer is not None or isinstance(f_value, AsyncIterator):
                external[path_str] = {
                    "value_stream": f_value,
                    "serializer": stream_serializer,
                    "storage": storage,
                }
            else:
                external[path_str] = {
                    "value": f_value,
                    "serializer": None,
                    "storage": storage,
                }
            field_dict[f_name] = LaboxRefDict(__labox__="ref", ref=path_str)
        elif serializer is not None:
            field_dict[f_name] = dump_content_dict(f_value, serializer)
        elif stream_serializer is not None or isinstance(f_value, AsyncIterator):
            path_str = "/".join((*path, f_name))
            external[path_str] = {
                "value_stream": f_value,
                "serializer": stream_serializer,
                "storage": storage,
            }
            field_dict[f_name] = LaboxRefDict(__labox__="ref", ref=path_str)
        else:
            field_dict[f_name] = _dump_any(
                f_value,
                registry=registry,
                external=external,
                path=(*path, f_name),
            )
    return _LaboxStorableDataclassDict(
        __labox__="storable_dataclass",
        class_id=obj.storable_config().class_id.hex,
        class_name=full_class_name(obj),
        fields=field_dict,
    )


def _dump_any(
    value: Any,
    *,
    registry: Registry,
    external: dict[str, AnyUnpackedValue],
    path: tuple[str, ...],
) -> Any:
    match value:
        case int() | str() | float() | bool() | None:
            return value
        case dict():
            return {
                k: _dump_any(v, registry=registry, external=external, path=(*path, k))
                for k, v in value.items()
            }
        case list() | tuple():
            return [
                _dump_any(v, registry=registry, external=external, path=(*path, str(i)))
                for i, v in enumerate(value)
            ]
        case StorableDataclass():
            return _dump_storable_dataclass(
                value,
                registry=registry,
                external=external,
                path=path,
            )
        case AsyncIterator():
            path_str = "/".join(path)
            external[path_str] = {
                "value_stream": value,
                "serializer": None,
                "storage": None,
            }
            return LaboxRefDict(__labox__="ref", ref=path_str)
        case _:
            path_str = "/".join(path)
            serializer = registry.get_serializer_by_type(type(value))
            return dump_content_dict(value, serializer)


class _LaboxStorableDataclassDict(TypedDict):
    """A dictionary representation of a storable dataclass."""

    __labox__: Literal["storable_dataclass"]
    class_id: str
    class_name: str
    fields: dict[str, Any]


def _load_storable_dataclass(
    dumped: _LaboxStorableDataclassDict,
    registry: Registry,
    external: Mapping[str, AnyUnpackedValue],
) -> StorableDataclass:
    """Load a storable dataclass from its dictionary representation."""
    dumped_fields = dumped["fields"]
    dumped_class_id = dumped["class_id"]
    cls = registry.get_storable(UUID(hex=dumped_class_id))

    kwargs: dict[str, Any] = {}
    for f in fields(cls):
        if not f.init:
            continue

        f_name = f.name
        if f_name not in dumped_fields:
            continue

        try:
            dumped_f = dumped_fields[f_name]
        except KeyError:
            msg = f"Missing required field '{f_name}' for {cls}."
            raise KeyError(msg) from None

        kwargs[f_name] = _load_any(
            dumped_f,
            registry=registry,
            external=external,
        )

    return cls(**kwargs)


def _load_any(
    value: Any,
    *,
    registry: Registry,
    external: Mapping[str, AnyUnpackedValue],
) -> Any:
    match value:
        case int() | str() | float() | bool() | None:
            return value
        case {"__labox__": "ref"}:
            return _load_ref_from_external(value["ref"], external)
        case {"__labox__": "content"}:
            return load_content_dict(value, registry)
        case {"__labox__": "storable_dataclass"}:
            return _load_storable_dataclass(value, registry, external)
        case dict():
            return {k: _load_any(v, registry=registry, external=external) for k, v in value.items()}
        case list() | tuple():
            return [_load_any(v, registry=registry, external=external) for v in value]
        case _:
            msg = f"Unexpected value while loading: {value}"
            raise ValueError(msg)


def _load_ref_from_external(ref_str: str, external: Mapping[str, AnyUnpackedValue]) -> Any:
    """Load a reference from the external dictionary."""
    if ref_str not in external:
        msg = f"Reference {ref_str} not found in external data."
        raise KeyError(msg)
    unpacked_value = external[ref_str]
    match unpacked_value:
        case {"value": value}:
            return value
        case {"value_stream": value_stream}:
            return value_stream
        case _:
            msg = f"Unexpected unpacked value for reference {ref_str}: {unpacked_value}"
            raise ValueError(msg)
