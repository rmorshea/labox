from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import Any
from typing import NotRequired
from typing import TypedDict
from typing import TypeGuard
from typing import TypeVar
from typing import Unpack
from typing import cast

from ruyaml import import_module

from labox._internal._utils import full_class_name
from labox._internal._utils import validate_typed_dict
from labox.common.exceptions import NotRegistered
from labox.core.serializer import Serializer
from labox.core.serializer import StreamSerializer
from labox.core.storable import Storable
from labox.core.storage import Storage
from labox.core.unpacker import Unpacker

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

    _normalized_attributes: _RegistryInfo
    """Normalized attributes for the registry, used for merging."""


class Registry:
    """A registry of storage schemes, serializers, and readers."""

    __slots__ = "_info"

    def __init__(self, **kwargs: Unpack[RegistryKwargs]) -> None:
        validate_typed_dict(RegistryKwargs, kwargs)
        self._info = _kwargs_to_info(kwargs)

    def get_default_storage(self) -> Storage:
        """Return the default storage for this registry."""
        if (s := self._info.get("default_storage")) is None:
            msg = "No default storage is set for this registry."
            raise ValueError(msg)
        return s

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
        ) is not None and cfg.class_id in self._info["storable_by_id"]:
            return True

        if raise_if_missing:
            msg = f"No storable class {cls} not found."
            raise NotRegistered(msg)

        return False

    def get_storable(self, class_id: UUID) -> type[Any]:
        """Get a storable class by its ID."""
        if cls := self._info["storable_by_id"].get(class_id):
            return cls
        msg = f"No storable class found with ID {class_id}."
        raise NotRegistered(msg)

    def get_serializer(self, name: str) -> Serializer:
        """Get a serializer by its name."""
        if serializer := self._info["serializer_by_name"].get(name):
            return serializer
        msg = f"No serializer found with name {name!r}."
        raise NotRegistered(msg)

    def get_stream_serializer(self, name: str) -> StreamSerializer:
        """Get a stream serializer by its name."""
        if serializer := self._info["stream_serializer_by_name"].get(name):
            return serializer
        msg = f"No stream serializer found with name {name!r}."
        raise NotRegistered(msg)

    def get_unpacker(self, name: str) -> Unpacker:
        """Get an unpacker by its name."""
        if unpacker := self._info["unpacker_by_name"].get(name):
            return unpacker
        msg = f"No unpacker found with name {name!r}."
        raise NotRegistered(msg)

    def get_storage(self, name: str) -> Storage:
        """Get a storage by its name."""
        if storage := self._info["storage_by_name"].get(name):
            return storage
        msg = f"No storage found with name {name!r}."
        raise NotRegistered(msg)

    def infer_unpacker(self, cls: type[S]) -> Unpacker[S]:
        """Get the first unpacker that can handle the given type or its parent classes."""
        unpacker = _infer_from_type(self._info["unpacker_by_type"], cls, "unpacker")
        return cast("Unpacker[S]", unpacker)

    def get_serializer_by_type(self, cls: type[T]) -> Serializer[T]:
        """Get a serializer that can handle the given type or its parent classes."""
        return _infer_from_type(self._info["serializer_by_type"], cls, "serializer")

    def get_serializer_by_content_type(self, content_type: str) -> Serializer:
        """Get a serializer that can handle the given content type."""
        parsed_ct = _parse_content_type(content_type)
        if serializer := self._info["serializer_by_content_type"].get(parsed_ct):
            return serializer
        msg = f"No serializer found for content type {content_type!r}."
        raise NotRegistered(msg)

    def get_stream_serializer_by_type(self, cls: type[T]) -> StreamSerializer[T]:
        """Get a stream serializer that can handle the given type or its base classes."""
        return _infer_from_type(self._info["stream_serializer_by_type"], cls, "stream serializer")

    def get_stream_serializer_by_content_type(self, content_type: str) -> StreamSerializer:
        """Get a stream serializer that can handle the given content type."""
        parsed_ct = _parse_content_type(content_type)
        if serializer := self._info["stream_serializer_by_content_type"].get(parsed_ct):
            return serializer
        msg = f"No stream serializer found for content type {content_type!r}."
        raise NotRegistered(msg)


def _infer_from_type(mapping: Mapping[type, V], cls: type, description: str) -> V:
    """Get the first value from the mapping that can handle the given type or its parent classes."""
    for base in cls.mro():
        if item := mapping.get(base):
            return item
    msg = f"No {description} found for {full_class_name(cls)}."
    raise ValueError(msg)


_DEFAULT_REGISTRY_ATTRS: _RegistryInfo = {
    "storable_by_id": {},
    "serializer_by_name": {},
    "serializer_by_type": {},
    "storage_by_name": {},
    "stream_serializer_by_name": {},
    "stream_serializer_by_type": {},
    "unpacker_by_name": {},
    "unpacker_by_type": {},
    "serializer_by_content_type": {},
    "stream_serializer_by_content_type": {},
}


def _kwargs_to_info(kwargs: RegistryKwargs) -> _RegistryInfo:
    infos_to_merge: list[_RegistryInfo] = []

    # normalized attributes have lowest priority
    if "_normalized_attributes" in kwargs:
        infos_to_merge.append(kwargs["_normalized_attributes"])

    # next are modules exports
    if (modules := kwargs.get("modules")) is not None:
        infos_to_merge.append(_kwargs_to_info(_kwargs_from_modules(modules)))

    # then other registries
    if (registries := kwargs.get("registries")) is not None:
        infos_to_merge.extend(reg._info for reg in registries)  # noqa: SLF001

    # then highest priority are explicitly given attributes
    infos_to_merge.append(_info_from_explicit_kwargs(kwargs))

    info = _merge_infos_with_ascending_priority(infos_to_merge)

    return _add_default_storage(info, kwargs)


def _info_from_explicit_kwargs(kwargs: RegistryKwargs) -> _RegistryInfo:
    serializer_by_content_type: dict[_ContentType, Serializer] = {}
    serializer_by_name: dict[str, Serializer] = {}
    serializer_by_type: dict[type[Any], Serializer] = {}
    storable_by_id: dict[UUID, type[Storable]] = {}
    storage_by_name: dict[str, Storage] = {}
    stream_serializer_by_content_type: dict[_ContentType, StreamSerializer] = {}
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

    for serializer in kwargs.get("serializers") or ():
        _check_name_defined_on_class(serializer)
        if isinstance(serializer, StreamSerializer):
            stream_serializer_by_name[serializer.name] = serializer
            stream_serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
            stream_serializer_by_content_type.update(
                dict.fromkeys(map(_parse_content_type, serializer.content_types), serializer)
            )

        else:
            serializer_by_name[serializer.name] = serializer
            serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
            serializer_by_content_type.update(
                dict.fromkeys(map(_parse_content_type, serializer.content_types), serializer)
            )

    for storage in kwargs.get("storages") or ():
        _check_name_defined_on_class(storage)
        storage_by_name[storage.name] = storage

    return {
        "serializer_by_content_type": serializer_by_content_type,
        "serializer_by_name": serializer_by_name,
        "serializer_by_type": serializer_by_type,
        "storable_by_id": storable_by_id,
        "storage_by_name": storage_by_name,
        "stream_serializer_by_content_type": stream_serializer_by_content_type,
        "stream_serializer_by_name": stream_serializer_by_name,
        "stream_serializer_by_type": stream_serializer_by_type,
        "unpacker_by_name": unpacker_by_name,
        "unpacker_by_type": unpacker_by_type,
    }


def _add_default_storage(info: _RegistryInfo, kwargs: RegistryKwargs) -> _RegistryInfo:
    default_storage: Storage | None = None
    storage_by_name = dict(info["storage_by_name"])

    # deal with default storage
    match kwargs.get("default_storage"):
        case Storage() as storage:
            # use the given storage as default and register it
            default_storage = storage
        case True:
            if storage_by_name:
                default_storage = tuple(storage_by_name.values())[-1]
        case False | None:
            default_storage = None
        case _:
            msg = "The 'default_storage' must be a Storage instance or True/False."
            raise TypeError(msg)

    if default_storage is not None:
        # Add it to the storage by name mapping (even if it was already there). This causes the
        # default storage to be the last and highest priority storage if no others are added when
        # merging registries.
        storage_by_name[default_storage.name] = default_storage

    return {**info, "storage_by_name": storage_by_name, "default_storage": default_storage}


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


def _merge_infos_with_ascending_priority(attrs: Sequence[_RegistryInfo]) -> _RegistryInfo:
    """Merge multiple registry attributes into a single one."""
    merged = deepcopy(_DEFAULT_REGISTRY_ATTRS)
    for r in attrs:
        for k, v in r.items():
            if isinstance(v, Mapping) and isinstance((merged_v := merged[k]), dict):
                merged_v.update(v)
            else:
                merged[k] = v
    return cast("_RegistryInfo", merged)


class _RegistryInfo(TypedDict):
    """Attributes for a registry."""

    default_storage: NotRequired[Storage | None]
    serializer_by_name: Mapping[str, Serializer]
    serializer_by_type: Mapping[type[Any], Serializer]
    storable_by_id: Mapping[UUID, type[Storable]]
    storage_by_name: Mapping[str, Storage]
    stream_serializer_by_name: Mapping[str, StreamSerializer]
    stream_serializer_by_type: Mapping[type[Any], StreamSerializer]
    unpacker_by_name: Mapping[str, Unpacker]
    unpacker_by_type: Mapping[type[Any], Unpacker]
    serializer_by_content_type: Mapping[_ContentType, Serializer]
    stream_serializer_by_content_type: Mapping[_ContentType, StreamSerializer]


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


_CONTENT_TYPE_REGEX = re.compile(
    r"^(?P<type>.+?)\/(?P<subtype>.+?)(?:\+(?P<suffix>.+?))?(?:;(?P<params>.*))?$",
)


def _parse_content_type(s: str) -> _ContentType:
    """Parse a MIME type string.

    ```
    mime-type = type "/" subtype ["+" suffix]* [";" parameter];
    ```
    """
    match = _CONTENT_TYPE_REGEX.match(s)
    if not match:
        msg = f"Invalid content type: {s!r}"
        raise ValueError(msg)

    type_ = match.group("type")
    subtype = match.group("subtype")
    suffix = match.group("suffix") or ""
    params_str = match.group("params") or ""

    parameters: list[tuple[str, str]] = []
    for param in params_str.split(";"):
        if not param.strip():
            continue
        key, _, value = param.partition("=")
        parameters.append((key.strip(), value.strip()))

    return _ContentType(
        type=type_,
        subtype=subtype,
        suffix=suffix,
        parameters=parameters,
    )


@dataclass(frozen=True)
class _ContentType:
    """A parsed content type."""

    type: str
    """The main type."""
    subtype: str
    """The sub-type."""
    suffix: str
    """The suffix, if any."""
    parameters: Sequence[tuple[str, str]] = field(hash=False)
    """Any additional parameters."""
