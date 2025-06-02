from __future__ import annotations

from inspect import isclass
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import TypedDict
from typing import TypeVar
from typing import Unpack
from typing import cast

from ruyaml import import_module

from lakery.core.scheme import StorageScheme
from lakery.core.scheme import StorageSchemeLoader
from lakery.core.serializer import Deserializer
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamDeserializer
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import Storage
from lakery.core.storage import StorageReader
from lakery.core.storage import StorageWriter

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Iterator
    from collections.abc import Mapping
    from collections.abc import Sequence
    from types import ModuleType
    from uuid import UUID

    from lakery.core.decomposer import Decomposer


T = TypeVar("T")
D = TypeVar("D", bound="Mapping[str, Mapping]")


class Registry:
    """A registry of storage schemes, serializers, and readers."""

    def __init__(self, **kwargs: Unpack[RegistryKwargs]) -> None:
        attrs = _kwargs_to_attrs(kwargs)
        self.storage_scheme_by_id = attrs["storage_scheme_by_id"]
        self.storage_scheme_loader_by_name = attrs["storage_scheme_loader_by_name"]
        self.serializer_by_type = attrs["serializer_by_type"]
        self.stream_serializer_by_type = attrs["stream_serializer_by_type"]
        self.deserializer_by_name = attrs["deserializer_by_name"]
        self.stream_deserializer_by_name = attrs["stream_deserializer_by_name"]
        self.storage_reader_by_name = attrs["storage_reader_by_name"]
        self.storage_writers = attrs["storage_writers"]
        if kwargs.get("default_storage_writer"):
            if not self.storage_writers:
                msg = "No storage writers registered, cannot set a default."
                raise ValueError(msg)
            self.default_storage_writer = self.storage_writers[0]

    @classmethod
    def from_modules(
        cls,
        *modules: str | ModuleType,
        default_storage_writer: bool = False,
    ) -> Self:
        """Create a registry from the given modules."""
        storage_schemes: list[type[StorageScheme]] = []
        storage_scheme_loaders: list[StorageSchemeLoader] = []
        serializers: list[Serializer | StreamSerializer] = []
        deserializers: list[Deserializer | StreamDeserializer] = []
        storage_writers: list[StorageWriter] = []
        storage_readers: list[StorageReader] = []

        for value in _iter_module_exports(modules):
            match value:
                case StorageSchemeLoader():
                    storage_scheme_loaders.append(value)
                case Serializer() | StreamSerializer():
                    serializers.append(value)
                case Deserializer() | StreamDeserializer():
                    deserializers.append(value)
                case StorageReader():
                    storage_readers.append(value)
                case StorageWriter():
                    storage_writers.append(value)
                case _:
                    if isclass(value) and issubclass(value, StorageScheme):
                        storage_schemes.append(value)

        return cls(
            storage_schemes=storage_schemes,
            storage_scheme_loaders=storage_scheme_loaders,
            serializers=serializers,
            deserializers=deserializers,
            storage_writers=storage_writers,
            storage_readers=storage_readers,
            default_storage_writer=default_storage_writer,
        )

    def merge(self, *others: Registry, **kwargs: Unpack[RegistryKwargs]) -> Self:
        """Return a new registry that merges this one with the given ones."""
        kwargs["_normalized_attributes"] = _merge_attrs_with_descending_priority(
            [
                {
                    "storage_scheme_by_id": o.storage_scheme_by_id,
                    "storage_scheme_loader_by_name": o.storage_scheme_loader_by_name,
                    "serializer_by_type": o.serializer_by_type,
                    "stream_serializer_by_type": o.stream_serializer_by_type,
                    "deserializer_by_name": o.deserializer_by_name,
                    "stream_deserializer_by_name": o.stream_deserializer_by_name,
                    "storage_reader_by_name": o.storage_reader_by_name,
                    "storage_writers": o.storage_writers,
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


class RegistryKwargs(TypedDict, total=False):
    """Arguments for creating a registry."""

    decomposer: Iterable[Decomposer]
    """Decomposers to register."""
    serializers: Iterable[Serializer | StreamSerializer]
    """Serializers to register."""
    storages: Iterable[Storage]
    """Storages to register."""
    use_default_storage: bool
    """Whether to set a default storage used when saving."""

    _normalized_attributes: _RegistryAttrs
    """Normalized attributes for the registry, used for merging."""


_DEFAULT_REGISTRY_ATTRS: _RegistryAttrs = {
    "storage_scheme_by_id": {},
    "storage_scheme_loader_by_name": {},
    "serializer_by_type": {},
    "stream_serializer_by_type": {},
    "deserializer_by_name": {},
    "stream_deserializer_by_name": {},
    "storage_reader_by_name": {},
    "storage_writers": (),
}


def _kwargs_to_attrs(kwargs: RegistryKwargs) -> _RegistryAttrs:
    attrs_from_kwargs = kwargs.get("_normalized_attributes") or _DEFAULT_REGISTRY_ATTRS
    storage_scheme_by_id = dict(attrs_from_kwargs["storage_scheme_by_id"])
    storage_scheme_loader_by_name = dict(attrs_from_kwargs["storage_scheme_loader_by_name"])
    serializer_by_type = dict(attrs_from_kwargs["serializer_by_type"])
    stream_serializer_by_type = dict(attrs_from_kwargs["stream_serializer_by_type"])
    deserializer_by_name = dict(attrs_from_kwargs["deserializer_by_name"])
    stream_deserializer_by_name = dict(attrs_from_kwargs["stream_deserializer_by_name"])
    storage_writers = tuple(kwargs.get("storage_writers", ()))
    storage_reader_by_name = dict(attrs_from_kwargs["storage_reader_by_name"])

    for scheme in kwargs.get("storage_schemes", ()):
        storage_scheme_by_id[scheme.storage_scheme_id()] = scheme
    for loader in kwargs.get("storage_scheme_loaders", ()):
        storage_scheme_loader_by_name[loader.name] = loader
    for serializer in reversed(tuple(kwargs.get("serializers", ()))):
        if isinstance(serializer, StreamSerializer):
            stream_deserializer_by_name[serializer.deserializer.name] = serializer.deserializer
            stream_serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
        else:
            deserializer_by_name[serializer.deserializer.name] = serializer.deserializer
            serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
    for deserializer in kwargs.get("deserializers", ()):
        if isinstance(deserializer, StreamDeserializer):
            stream_deserializer_by_name[deserializer.name] = deserializer
        else:
            deserializer_by_name[deserializer.name] = deserializer
    for writer in storage_writers:
        storage_reader_by_name[writer.reader.name] = writer.reader
    for reader in kwargs.get("storage_readers", ()):
        storage_reader_by_name[reader.name] = reader

    return {
        "storage_scheme_by_id": storage_scheme_by_id,
        "storage_scheme_loader_by_name": storage_scheme_loader_by_name,
        "serializer_by_type": serializer_by_type,
        "stream_serializer_by_type": stream_serializer_by_type,
        "deserializer_by_name": deserializer_by_name,
        "stream_deserializer_by_name": stream_deserializer_by_name,
        "storage_writers": storage_writers,
        "storage_reader_by_name": storage_reader_by_name,
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

    storage_scheme_by_id: Mapping[UUID, type[StorageScheme]]
    """Storage schemes by their UUID."""
    storage_scheme_loader_by_name: Mapping[str, StorageSchemeLoader]
    """Storage scheme loaders by their name."""
    serializer_by_type: Mapping[type[Any], Serializer]
    """Serializers by the type they can handle."""
    stream_serializer_by_type: Mapping[type[Any], StreamSerializer]
    """Stream serializers by the type they can handle."""
    deserializer_by_name: Mapping[str, Deserializer]
    """Deserializers by their name."""
    stream_deserializer_by_name: Mapping[str, StreamDeserializer]
    """Stream deserializers by their name."""
    storage_writers: Sequence[StorageWriter]
    """Storage writers."""
    storage_reader_by_name: Mapping[str, StorageReader]
    """Storage readers by their name."""


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
