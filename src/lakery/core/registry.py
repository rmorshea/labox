from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import TypedDict
from typing import TypeVar
from typing import Unpack
from typing import cast

from ruyaml import import_module

from lakery._internal.utils import full_class_name
from lakery.common.exceptions import NotRegistered
from lakery.core.model import get_model_info
from lakery.core.model import has_model_info
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
V = TypeVar("V")
D = TypeVar("D", bound="Mapping[str, Mapping]")


class RegistryKwargs(TypedDict, total=False):
    """Arguments for creating a registry."""

    models: Iterable[type[Any]]
    """Models to register."""
    unpackers: Iterable[Unpacker]
    """Decomposers to register."""
    serializers: Iterable[Serializer | StreamSerializer]
    """Serializers to register."""
    storages: Iterable[Storage]
    """Storages to register."""
    use_type_inference: bool
    """Whether to use type inference when looking up serializers and unpackers (default: True)."""
    use_default_storage: bool
    """Whether to set a default storage used when saving (default: False)."""

    _normalized_attributes: _RegistryAttrs
    """Normalized attributes for the registry, used for merging."""


class Registry:
    """A registry of storage schemes, serializers, and readers."""

    def __init__(self, **kwargs: Unpack[RegistryKwargs]) -> None:
        attrs = _kwargs_to_attrs(kwargs)
        self._default_storage = attrs["default_storage"]
        self.model_by_id = attrs["model_by_id"]
        self.serializer_by_name = attrs["serializer_by_name"]
        self.serializer_by_type = attrs["serializer_by_type"]
        self.storage_by_name = attrs["storage_by_name"]
        self.stream_serializer_by_name = attrs["stream_serializer_by_name"]
        self.stream_serializer_by_type = attrs["stream_serializer_by_type"]
        self.unpacker_by_name = attrs["unpacker_by_name"]
        self.unpacker_by_type = attrs["unpacker_by_type"]
        self.use_type_inference = kwargs.get("use_type_inference", True)

    def get_default_storage(self) -> Storage:
        """Return the default storage for this registry."""
        if self._default_storage is None:
            msg = "No default storage is set for this registry."
            raise ValueError(msg)
        return self._default_storage

    @classmethod
    def from_modules(
        cls,
        *modules: str | ModuleType,
        use_default_storage: bool = False,
    ) -> Self:
        """Create a registry from the given modules."""
        models: list[type[Any]] = []
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
                    if has_model_info(value):
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
                    "default_storage": o._default_storage,  # noqa: SLF001
                    "model_by_id": o.model_by_id,
                    "serializer_by_name": o.serializer_by_name,
                    "serializer_by_type": o.serializer_by_type,
                    "storage_by_name": o.storage_by_name,
                    "stream_serializer_by_name": o.stream_serializer_by_name,
                    "stream_serializer_by_type": o.stream_serializer_by_type,
                    "unpacker_by_name": o.unpacker_by_name,
                    "unpacker_by_type": o.unpacker_by_type,
                }
                for o in (*others, self)
            ]
        )
        return self.__class__(**kwargs)

    def check_model_registered(self, model: type[Any]) -> None:
        """Check if the given model is registered in this registry."""
        if not has_model_info(model):
            msg = f"The model {full_class_name(model)} must have a model ID."
            raise ValueError(msg)
        if get_model_info(model) not in self.model_by_id:
            msg = f"The model {full_class_name(model)} is not registered in this registry."
            raise NotRegistered(msg)

    def get_model(self, model_id: UUID) -> type[Any]:
        """Get a model by its ID."""
        if model := self.model_by_id.get(model_id):
            return model
        msg = f"No model found with ID '{model_id}'."
        raise NotRegistered(msg)

    def get_serializer(self, name: str) -> Serializer:
        """Get a serializer by its name."""
        if serializer := self.serializer_by_name.get(name):
            return serializer
        msg = f"No serializer found with name '{name}'."
        raise NotRegistered(msg)

    def get_stream_serializer(self, name: str) -> StreamSerializer:
        """Get a stream serializer by its name."""
        if serializer := self.stream_serializer_by_name.get(name):
            return serializer
        msg = f"No stream serializer found with name '{name}'."
        raise NotRegistered(msg)

    def get_unpacker(self, name: str) -> Unpacker:
        """Get an unpacker by its name."""
        if unpacker := self.unpacker_by_name.get(name):
            return unpacker
        msg = f"No unpacker found with name '{name}'."
        raise NotRegistered(msg)

    def get_storage(self, name: str) -> Storage:
        """Get a storage by its name."""
        if storage := self.storage_by_name.get(name):
            return storage
        msg = f"No storage found with name '{name}'."
        raise NotRegistered(msg)

    def infer_unpacker(self, cls: type[T]) -> Unpacker[T]:
        """Get the first unpacker that can handle the given type or its parent classes."""
        self._check_allow_type_inference()
        return _infer_from_type(self.unpacker_by_type, cls, "unpacker")

    def infer_serializer(self, cls: type[T]) -> Serializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        self._check_allow_type_inference()
        return _infer_from_type(self.serializer_by_type, cls, "serializer")

    def infer_stream_serializer(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first stream serializer that can handle the given type or its base classes."""
        self._check_allow_type_inference()
        return _infer_from_type(self.stream_serializer_by_type, cls, "stream serializer")

    def _check_allow_type_inference(self) -> None:
        """Check if type inference is allowed."""
        if not self.use_type_inference:
            msg = "Type inference is disabled for this registry."
            raise ValueError(msg)


def _infer_from_type(mapping: Mapping[type[T], V], cls: type[T], description: str) -> V:
    """Get the first value from the mapping that can handle the given type or its parent classes."""
    for base in cls.mro():
        if item := mapping.get(base):
            return item
    msg = f"No {description} found for {full_class_name(cls)}."
    raise ValueError(msg)


_DEFAULT_REGISTRY_ATTRS: _RegistryAttrs = {
    "default_storage": None,
    "model_by_id": {},
    "serializer_by_name": {},
    "serializer_by_type": {},
    "storage_by_name": {},
    "stream_serializer_by_name": {},
    "stream_serializer_by_type": {},
    "unpacker_by_name": {},
    "unpacker_by_type": {},
}


def _kwargs_to_attrs(kwargs: RegistryKwargs) -> _RegistryAttrs:
    attrs_from_kwargs = kwargs.get("_normalized_attributes") or _DEFAULT_REGISTRY_ATTRS
    unpacker_by_name = dict(attrs_from_kwargs["unpacker_by_name"])
    unpacker_by_type = dict(attrs_from_kwargs.get("unpacker_by_type", {}))
    default_storage = attrs_from_kwargs["default_storage"]
    model_by_id = dict(attrs_from_kwargs["model_by_id"])
    serializer_by_name = dict(attrs_from_kwargs["serializer_by_name"])
    serializer_by_type = dict(attrs_from_kwargs["serializer_by_type"])
    storage_by_name = dict(attrs_from_kwargs["storage_by_name"])
    stream_serializer_by_name = dict(attrs_from_kwargs["stream_serializer_by_name"])
    stream_serializer_by_type = dict(attrs_from_kwargs["stream_serializer_by_type"])

    for unpacker in kwargs.get("unpackers", ()):
        _check_name_defined_on_class(unpacker)
        unpacker_by_name[unpacker.name] = unpacker
    for model in kwargs.get("models", ()):
        model_by_id[get_model_info(model).model_id] = model
    for serializer in reversed(tuple(kwargs.get("serializers", ()))):
        _check_name_defined_on_class(serializer)
        if isinstance(serializer, StreamSerializer):
            stream_serializer_by_name[serializer.name] = serializer
            stream_serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
        else:
            serializer_by_name[serializer.name] = serializer
            serializer_by_type.update(dict.fromkeys(serializer.types, serializer))
    for storage in reversed(tuple(kwargs.get("storages", ()))):  # reverse to make first the default
        _check_name_defined_on_class(storage)
        storage_by_name[storage.name] = storage
        if kwargs.get("use_default_storage"):
            default_storage = storage

    return {
        "unpacker_by_name": unpacker_by_name,
        "unpacker_by_type": unpacker_by_type,
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

    default_storage: Storage | None
    model_by_id: Mapping[UUID, type[Any]]
    serializer_by_name: Mapping[str, Serializer]
    serializer_by_type: Mapping[type[Any], Serializer]
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
