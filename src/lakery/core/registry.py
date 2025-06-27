from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING
from typing import Any
from typing import NotRequired
from typing import TypedDict
from typing import TypeGuard
from typing import TypeVar
from typing import Unpack
from typing import cast

from ruyaml import import_module

from lakery._internal._utils import full_class_name
from lakery._internal._utils import validate_typed_dict
from lakery.common.exceptions import NotRegistered
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer
from lakery.core.storable import Storable
from lakery.core.storage import Storage
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Iterator
    from collections.abc import Sequence
    from types import ModuleType
    from uuid import UUID


T = TypeVar("T")
S = TypeVar("S", bound=Storable)
V = TypeVar("V")
D = TypeVar("D", bound="Mapping[str, Mapping]")


class RegistryKwargs(TypedDict, total=False):
    """Arguments for creating a registry."""

    modules: Sequence[str | ModuleType] | None
    """Modules to import and extract registry attributes from."""
    registries: Sequence[Registry] | None
    """Other registries to merge with this one."""
    storables: Sequence[type[Storable]] | None
    """Storable classes to register."""
    unpackers: Sequence[Unpacker] | None
    """Decomposers to register."""
    serializers: Sequence[Serializer | StreamSerializer] | None
    """Serializers to register."""
    storages: Sequence[Storage] | None
    """Storages to register."""
    default_storage: bool | Storage
    """Whether to set a default storage used when saving (default: False)."""
    infer_serializers: bool
    """Whether to infer serializers from the type of the storable (default: True)."""
    infer_unpackers: bool
    """Whether to infer unpackers from the type of the storable (default: True)."""

    _normalized_attributes: _RegistryAttrs
    """Normalized attributes for the registry, used for merging."""


class Registry:
    """A registry of storage schemes, serializers, and readers."""

    def __init__(self, **kwargs: Unpack[RegistryKwargs]) -> None:
        validate_typed_dict(RegistryKwargs, kwargs)

        attrs = _kwargs_to_attrs(kwargs)
        self._default_storage = attrs.get("default_storage")
        self.storable_by_id = attrs["storable_by_id"]
        self.serializer_by_name = attrs["serializer_by_name"]
        self.serializer_by_type = attrs["serializer_by_type"]
        self.storage_by_name = attrs["storage_by_name"]
        self.stream_serializer_by_name = attrs["stream_serializer_by_name"]
        self.stream_serializer_by_type = attrs["stream_serializer_by_type"]
        self.unpacker_by_name = attrs["unpacker_by_name"]
        self.unpacker_by_type = attrs["unpacker_by_type"]
        self.infer_serializers = kwargs.get("infer_serializers", True)
        self.infer_unpackers = kwargs.get("infer_unpackers", True)

    def get_default_storage(self) -> Storage:
        """Return the default storage for this registry."""
        if self._default_storage is None:
            msg = "No default storage is set for this registry."
            raise ValueError(msg)
        return self._default_storage

    def has_storable(
        self,
        cls: type[Any],
        *,
        raise_if_missing: bool = False,
    ) -> TypeGuard[type[Storable]]:
        """Check if the given class is registered in this registry."""
        if not issubclass(cls, Storable):
            msg = f"The class {full_class_name(cls)} is not a storable class."
            raise TypeError(msg)
        if (
            cfg := cls.get_storable_config(allow_none=True)
        ) is not None and cfg.class_id in self.storable_by_id:
            return True

        if raise_if_missing:
            msg = f"No storable class {cls} not found."
            raise NotRegistered(msg)

        return False

    def get_storable(self, class_id: UUID) -> type[Any]:
        """Get a storable class by its ID."""
        if cls := self.storable_by_id.get(class_id):
            return cls
        msg = f"No storable class found with ID {class_id}."
        raise NotRegistered(msg)

    def get_serializer(self, name: str) -> Serializer:
        """Get a serializer by its name."""
        if serializer := self.serializer_by_name.get(name):
            return serializer
        msg = f"No serializer found with name {name!r}."
        raise NotRegistered(msg)

    def get_stream_serializer(self, name: str) -> StreamSerializer:
        """Get a stream serializer by its name."""
        if serializer := self.stream_serializer_by_name.get(name):
            return serializer
        msg = f"No stream serializer found with name {name!r}."
        raise NotRegistered(msg)

    def get_unpacker(self, name: str) -> Unpacker:
        """Get an unpacker by its name."""
        if unpacker := self.unpacker_by_name.get(name):
            return unpacker
        msg = f"No unpacker found with name {name!r}."
        raise NotRegistered(msg)

    def get_storage(self, name: str) -> Storage:
        """Get a storage by its name."""
        if storage := self.storage_by_name.get(name):
            return storage
        msg = f"No storage found with name {name!r}."
        raise NotRegistered(msg)

    def infer_unpacker(self, cls: type[S]) -> Unpacker[S]:
        """Get the first unpacker that can handle the given type or its parent classes."""
        if not self.infer_unpackers:
            msg = "Type inference for unpackers is disabled for this registry."
            raise ValueError(msg)
        return cast("Unpacker[S]", _infer_from_type(self.unpacker_by_type, cls, "unpacker"))

    def infer_serializer(self, cls: type[T]) -> Serializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        if not self.infer_serializers:
            msg = "Type inference for serializers is disabled for this registry."
            raise ValueError(msg)
        return _infer_from_type(self.serializer_by_type, cls, "serializer")

    def infer_stream_serializer(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first stream serializer that can handle the given type or its base classes."""
        if not self.infer_serializers:
            msg = "Type inference for serializers is disabled for this registry."
            raise ValueError(msg)
        return _infer_from_type(self.stream_serializer_by_type, cls, "stream serializer")


def _infer_from_type(mapping: Mapping[type, V], cls: type, description: str) -> V:
    """Get the first value from the mapping that can handle the given type or its parent classes."""
    for base in cls.mro():
        if item := mapping.get(base):
            return item
    msg = f"No {description} found for {full_class_name(cls)}."
    raise ValueError(msg)


_DEFAULT_REGISTRY_ATTRS: _RegistryAttrs = {
    "storable_by_id": {},
    "serializer_by_name": {},
    "serializer_by_type": {},
    "storage_by_name": {},
    "stream_serializer_by_name": {},
    "stream_serializer_by_type": {},
    "unpacker_by_name": {},
    "unpacker_by_type": {},
    "infer_serializers": True,
    "infer_unpackers": True,
}


def _kwargs_from_modules(
    modules: Iterable[ModuleType | str],
) -> RegistryKwargs:
    """Extract registry kwargs from the given modules."""
    unpackers: list[Unpacker] = []
    storables: list[type[Storable]] = []
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
                if (
                    issubclass(value, Storable)
                    and value.get_storable_config(allow_none=True) is not None
                ):
                    storables.append(value)

    return {
        "storables": storables,
        "unpackers": unpackers,
        "serializers": serializers,
        "storages": storages,
    }


def _kwargs_to_attrs(kwargs: RegistryKwargs) -> _RegistryAttrs:
    infer_serializers = kwargs.get("infer_serializers", True)
    infer_unpackers = kwargs.get("infer_unpackers", True)
    serializer_by_name: dict[str, Serializer] = {}
    serializer_by_type: dict[type[Any], Serializer] = {}
    storable_by_id: dict[UUID, type[Storable]] = {}
    storage_by_name: dict[str, Storage] = {}
    stream_serializer_by_name: dict[str, StreamSerializer] = {}
    stream_serializer_by_type: dict[type[Any], StreamSerializer] = {}
    unpacker_by_name: dict[str, Unpacker] = {}
    unpacker_by_type: dict[type[Any], Unpacker] = {}

    for cls in kwargs.get("storables") or ():
        cfg = cls.get_storable_config()
        unpacker_by_type[cls] = cfg.unpacker
        unpacker_by_name[cfg.unpacker.name] = cfg.unpacker
        storable_by_id[cfg.class_id] = cls

    for unpacker in kwargs.get("unpackers") or ():
        _check_name_defined_on_class(unpacker)
        unpacker_by_name[unpacker.name] = unpacker
        for cls in unpacker.types:
            unpacker_by_type[cls] = unpacker

    for serializer in kwargs.get("serializers") or ():
        _check_name_defined_on_class(serializer)
        if isinstance(serializer, StreamSerializer):
            stream_serializer_by_name[serializer.name] = serializer
            stream_serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
        else:
            serializer_by_name[serializer.name] = serializer
            serializer_by_type.update(dict.fromkeys(serializer.types, serializer))

    for storage in kwargs.get("storages") or ():
        _check_name_defined_on_class(storage)
        storage_by_name[storage.name] = storage

    attrs_to_merge: list[_RegistryAttrs] = []

    # normalized attributes have lowest priority
    if "_normalized_attributes" in kwargs:
        attrs_to_merge.append(kwargs["_normalized_attributes"])

    # next are modules exports
    if (modules := kwargs.get("modules")) is not None:
        attrs_to_merge.append(_kwargs_to_attrs(_kwargs_from_modules(modules)))

    # then other registries
    if (registries := kwargs.get("registries")) is not None:
        attrs_to_merge.extend(
            {
                "default_storage": reg._default_storage,  # noqa: SLF001
                "storable_by_id": reg.storable_by_id,
                "serializer_by_name": reg.serializer_by_name,
                "serializer_by_type": reg.serializer_by_type,
                "storage_by_name": reg.storage_by_name,
                "stream_serializer_by_name": reg.stream_serializer_by_name,
                "stream_serializer_by_type": reg.stream_serializer_by_type,
                "unpacker_by_name": reg.unpacker_by_name,
                "unpacker_by_type": reg.unpacker_by_type,
                "infer_serializers": reg.infer_serializers,
                "infer_unpackers": reg.infer_unpackers,
            }
            for reg in registries
        )

    # then highest priority are explicitly given attributes
    attrs_to_merge.append(
        {
            "infer_serializers": infer_serializers,
            "infer_unpackers": infer_unpackers,
            "serializer_by_name": serializer_by_name,
            "serializer_by_type": serializer_by_type,
            "storable_by_id": storable_by_id,
            "storage_by_name": storage_by_name,
            "stream_serializer_by_name": stream_serializer_by_name,
            "stream_serializer_by_type": stream_serializer_by_type,
            "unpacker_by_name": unpacker_by_name,
            "unpacker_by_type": unpacker_by_type,
        }
    )

    attrs = _merge_attrs_with_ascending_priority(attrs_to_merge)

    # deal with default storage
    match kwargs.get("default_storage", True):
        case Storage() as storage:
            # use the given storage as default and register it
            attrs["default_storage"] = storage
            storage_by_name[storage.name] = storage
        case True:
            if attrs["storage_by_name"]:
                attrs["default_storage"] = tuple(attrs["storage_by_name"].values())[-1]
        case None:
            attrs["default_storage"] = None
        case _:
            msg = "The 'default_storage' must be a Storage instance, True, or None."
            raise TypeError(msg)

    return attrs


def _merge_attrs_with_ascending_priority(attrs: Sequence[_RegistryAttrs]) -> _RegistryAttrs:
    """Merge multiple registry attributes into a single one."""
    merged = deepcopy(_DEFAULT_REGISTRY_ATTRS)
    for r in attrs:
        for k, v in r.items():
            if isinstance(v, Mapping) and isinstance((merged_v := merged[k]), dict):
                merged_v.update(v)
            else:
                merged[k] = v
    return cast("_RegistryAttrs", merged)


class _RegistryAttrs(TypedDict):
    """Attributes for a registry."""

    default_storage: NotRequired[Storage | None]
    infer_serializers: bool
    infer_unpackers: bool
    serializer_by_name: Mapping[str, Serializer]
    serializer_by_type: Mapping[type[Any], Serializer]
    storable_by_id: Mapping[UUID, type[Storable]]
    storage_by_name: Mapping[str, Storage]
    stream_serializer_by_name: Mapping[str, StreamSerializer]
    stream_serializer_by_type: Mapping[type[Any], StreamSerializer]
    unpacker_by_name: Mapping[str, Unpacker]
    unpacker_by_type: Mapping[type[Any], Unpacker]


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


def _check_name_defined_on_class(obj: Any) -> None:
    if hasattr(type(obj), "name"):
        return
    if hasattr(obj, "name"):
        msg = f"The 'name' of {obj} must be defined on the class, not the instance."
        raise ValueError(msg)
    msg = f"The {obj} has no 'name' attribute defined."
    raise ValueError(msg)
