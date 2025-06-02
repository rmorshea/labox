from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import TypedDict
from typing import TypeVar
from typing import Unpack
from typing import cast

from ruyaml import import_module

from lakery.core.model import BaseModel
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import Storage
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Iterator
    from collections.abc import Mapping
    from collections.abc import Sequence
    from types import ModuleType
    from uuid import UUID


T = TypeVar("T")
D = TypeVar("D", bound="Mapping[str, Mapping]")


class RegistryKwargs(TypedDict, total=False):
    """Arguments for creating a registry."""

    models: Iterable[type[BaseModel]]
    """Models to register."""
    unpackers: Iterable[Unpacker]
    """Decomposers to register."""
    serializers: Iterable[Serializer | StreamSerializer]
    """Serializers to register."""
    storages: Iterable[Storage]
    """Storages to register."""
    use_default_storage: bool
    """Whether to set a default storage used when saving (default: False)."""

    _normalized_attributes: _RegistryAttrs
    """Normalized attributes for the registry, used for merging."""


class Registry:
    """A registry of storage schemes, serializers, and readers."""

    def __init__(self, **kwargs: Unpack[RegistryKwargs]) -> None:
        attrs = _kwargs_to_attrs(kwargs)
        self.unpacker_by_name = attrs["unpacker_by_name"]
        self.default_storage = attrs["default_storage"]
        self.model_by_id = attrs["model_by_id"]
        self.serializer_by_name = attrs["serializer_by_name"]
        self.serializer_by_type = attrs["serializer_by_type"]
        self.storage_by_name = attrs["storage_by_name"]
        self.stream_serializer_by_name = attrs["stream_serializer_by_name"]
        self.stream_serializer_by_type = attrs["stream_serializer_by_type"]

    @classmethod
    def from_modules(
        cls,
        *modules: str | ModuleType,
        use_default_storage: bool = False,
    ) -> Self:
        """Create a registry from the given modules."""
        models: list[type[BaseModel]] = []
        unpackers: list[Unpacker] = []
        serializers: list[Serializer | StreamSerializer] = []
        storages: list[Storage] = []

        for value in _iter_module_exports(modules):
            match value:
                case Storage():
                    storages.append(value)
                case Serializer() | StreamSerializer():
                    serializers.append(value)
                case Unpacker():
                    unpackers.append(value)
                case type():
                    if issubclass(value, BaseModel):
                        models.append(value)

        return cls(
            models=models,
            unpackers=unpackers,
            serializers=serializers,
            storages=storages,
            use_default_storage=use_default_storage,
        )

    def merge(self, *others: Registry, **kwargs: Unpack[RegistryKwargs]) -> Self:
        """Return a new registry that merges this one with the given ones."""
        kwargs["_normalized_attributes"] = _merge_attrs_with_descending_priority(
            [
                {
                    "model_by_id": o.model_by_id,
                    "unpacker_by_name": o.unpacker_by_name,
                    "serializer_by_name": o.serializer_by_name,
                    "stream_serializer_by_name": o.stream_serializer_by_name,
                    "storage_by_name": o.storage_by_name,
                    "default_storage": o.default_storage,
                    "serializer_by_type": o.serializer_by_type,
                    "stream_serializer_by_type": o.stream_serializer_by_type,
                }
                for o in (*others, self)
            ]
        )
        return self.__class__(**kwargs)

    def infer_serializer(self, cls: type[T]) -> Serializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self.serializer_by_type.get(base):
                return item
        msg = f"No serializer found for {cls}."
        raise ValueError(msg)

    def infer_stream_serializer(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first stream serializer that can handle the given type or its base classes."""
        for base in cls.mro():
            if item := self.stream_serializer_by_type.get(base):
                return item
        msg = f"No stream serializer found for {cls}."
        raise ValueError(msg)


_DEFAULT_REGISTRY_ATTRS: _RegistryAttrs = {
    "unpacker_by_name": {},
    "default_storage": None,
    "model_by_id": {},
    "serializer_by_name": {},
    "serializer_by_type": {},
    "storage_by_name": {},
    "stream_serializer_by_name": {},
    "stream_serializer_by_type": {},
}


def _kwargs_to_attrs(kwargs: RegistryKwargs) -> _RegistryAttrs:
    attrs_from_kwargs = kwargs.get("_normalized_attributes") or _DEFAULT_REGISTRY_ATTRS
    unpacker_by_name = dict(attrs_from_kwargs["unpacker_by_name"])
    default_storage = attrs_from_kwargs["default_storage"]
    model_by_id = dict(attrs_from_kwargs["model_by_id"])
    serializer_by_name = dict(attrs_from_kwargs["serializer_by_name"])
    serializer_by_type = dict(attrs_from_kwargs["serializer_by_type"])
    storage_by_name = dict(attrs_from_kwargs["storage_by_name"])
    stream_serializer_by_name = dict(attrs_from_kwargs["stream_serializer_by_name"])
    stream_serializer_by_type = dict(attrs_from_kwargs["stream_serializer_by_type"])

    for unpacker in kwargs.get("unpacker", ()):
        unpacker_by_name[unpacker.name] = unpacker
    for model in kwargs.get("models", ()):
        model_by_id[model.model_class_id()] = model
    for serializer in reversed(tuple(kwargs.get("serializers", ()))):
        if isinstance(serializer, StreamSerializer):
            stream_serializer_by_name[serializer.name] = serializer
            stream_serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
        else:
            serializer_by_name[serializer.name] = serializer
            serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
    for storage in reversed(tuple(kwargs.get("storages", ()))):  # reverse to make first the default
        storage_by_name[storage.name] = storage
        if kwargs.get("use_default_storage"):
            default_storage = storage

    return {
        "unpacker_by_name": unpacker_by_name,
        "default_storage": default_storage,
        "model_by_id": model_by_id,
        "serializer_by_name": serializer_by_name,
        "serializer_by_type": serializer_by_type,
        "storage_by_name": storage_by_name,
        "stream_serializer_by_name": stream_serializer_by_name,
        "stream_serializer_by_type": stream_serializer_by_type,
    }


def _merge_attrs_with_descending_priority(attrs: Sequence[_RegistryAttrs]) -> _RegistryAttrs:
    """Merge multiple registry attributes into a single one."""
    merged: dict[str, dict] = {k: dict(cast("Any", v)) for k, v in _DEFAULT_REGISTRY_ATTRS.items()}
    for r in attrs:
        for k, v in r.items():
            merged[k].update(cast("Any", v))
    return cast("_RegistryAttrs", merged)


class _RegistryAttrs(TypedDict):
    """Attributes for a registry."""

    unpacker_by_name: Mapping[str, Unpacker]
    default_storage: Storage | None
    model_by_id: Mapping[UUID, type[BaseModel]]
    serializer_by_name: Mapping[str, Serializer]
    serializer_by_type: Mapping[type[Any], Serializer]
    storage_by_name: Mapping[str, Storage]
    stream_serializer_by_name: Mapping[str, StreamSerializer]
    stream_serializer_by_type: Mapping[type[Any], StreamSerializer]


def _iter_module_exports(modules: Iterable[ModuleType | str]) -> Iterator[Any]:
    """Iterate over all exports from the given modules."""
    for mod in modules:
        if isinstance(mod, str):
            mod = import_module(mod)
        if not hasattr(mod, "__all__"):
            msg = f"Module {mod} must have an '__all__' attribute."
            raise ValueError(msg)
        for name in mod.__all__:
            yield getattr(mod, name)
