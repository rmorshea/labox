from __future__ import annotations

import abc
from collections import Counter
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from importlib import import_module
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Self
from typing import TypeVar
from uuid import UUID

from typing_extensions import TypeIs

from lakery.common.exceptions import NotRegistered
from lakery.common.utils import frozenclass
from lakery.core.model import BaseStorageModel
from lakery.core.serializer import Serializer
from lakery.core.serializer import StreamSerializer
from lakery.core.storage import Storage

if TYPE_CHECKING:
    from collections.abc import Sequence
    from types import ModuleType


K = TypeVar("K")
V = TypeVar("V")
T = TypeVar("T")


class _BaseRegistry(Mapping[K, V], abc.ABC):
    """A registry of named items."""

    value_description: ClassVar[str]
    """A description for the type of value"""

    def __init__(self, values: Iterable[V] = (), /, *, ignore_conflicts: bool = False) -> None:
        items = [(self.get_key(i), i) for i in values]

        if not ignore_conflicts and (
            conflicts := {n for n, c in Counter(k for k, _ in items).items() if c > 1}
        ):
            msg = f"Conflicting {self.value_description.lower()} keys: {conflicts}"
            raise ValueError(msg)

        self._entries: Mapping[K, V] = dict(items)

    @abc.abstractmethod
    def get_key(self, value: V, /) -> K:
        """Get the key for the given value."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def can_register(cls, value: Any, /) -> TypeIs[V]:
        """Return whether the given value is a valid registry value."""
        raise NotImplementedError

    @classmethod
    def merge(cls, *registries: Any, ignore_conflicts: bool = False) -> Self:
        """Return a new registry that merges this one with the given ones."""
        new_values = [v for r in registries for v in r.values()]
        return cls(new_values, ignore_conflicts=ignore_conflicts)

    def check_registered(self, value: V) -> None:
        """Ensure that the given value is registered - raises a ValueError if not."""
        if not self.is_registered(value):
            key = self.get_key(value)
            msg = f"{self.value_description} {value!r} with key {key!r} is not registered."
            raise NotRegistered(msg)

    def is_registered(self, value: V) -> bool:
        """Return whether the given value is registered."""
        if (key := self.get_key(value)) not in self._entries:
            return False
        return self._entries[key] is value

    @classmethod
    def from_modules(
        cls,
        *modules: ModuleType | str,
        ignore_conflicts: bool = False,
        **kwargs: Any,
    ) -> Self:
        """Create a registry from a module."""
        model_types: list[V] = []
        for mod in modules:
            if isinstance(mod, str):
                mod = import_module(mod)
            if not hasattr(mod, "__all__"):
                msg = f"Module {mod} must have an '__all__' attribute."
                raise ValueError(msg)
            for name in mod.__all__:
                maybe = getattr(mod, name, None)
                if cls.can_register(maybe):
                    model_types.append(maybe)
        return cls(model_types, ignore_conflicts=ignore_conflicts, **kwargs)

    def __getitem__(self, key: K) -> V:
        try:
            return self._entries[key]
        except KeyError:
            msg = f"{self.value_description} {key!r} is not registered."
            raise NotRegistered(msg) from None

    def __iter__(self) -> Iterator[K]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)

    def __hash__(self) -> int:
        return hash((type(self), tuple(self._entries)))

    def __repr__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self._entries.items())
        return f"{type(self).__name__}({items})"


class ModelRegistry(_BaseRegistry[UUID, type[BaseStorageModel]]):
    """A registry of storage model types."""

    value_description = "Storage model type"

    def get_key(self, model: type[BaseStorageModel]) -> UUID:
        """Get the key for the given model."""
        return model.storage_model_config().id

    @classmethod
    def can_register(cls, value: Any) -> TypeIs[type[BaseStorageModel]]:
        """Return whether the given value is a valid serializer."""
        return (
            isinstance(value, type)
            and issubclass(value, BaseStorageModel)
            and value.storage_model_config(allow_missing=True) is not None
        )


class SerializerRegistry(_BaseRegistry[str, Serializer | StreamSerializer]):
    """A registry of stream serializers."""

    value_description = "Serializer"

    def __init__(
        self,
        serializers: Sequence[Serializer | StreamSerializer] = (),
        *,
        ignore_conflicts: bool = False,
    ) -> None:
        super().__init__(serializers, ignore_conflicts=ignore_conflicts)

        non_stream_serializers: list[Serializer] = []
        stream_serializers: list[StreamSerializer] = []
        for serializer in serializers:
            if isinstance(serializer, StreamSerializer):
                stream_serializers.append(serializer)
            else:
                non_stream_serializers.append(serializer)

        self._by_value_type = {
            type_: serializer
            for serializer in [*stream_serializers, *non_stream_serializers]
            for type_ in serializer.types
        }
        self._by_stream_type = {
            type_: serializer for serializer in stream_serializers for type_ in serializer.types
        }

    def get_key(self, serializer: Serializer | StreamSerializer) -> str:
        """Get the key for the given serializer."""
        return serializer.name

    @classmethod
    def can_register(cls, value: Any) -> TypeIs[Serializer | StreamSerializer]:
        """Return whether the given value is a valid serializer."""
        return isinstance(value, Serializer | StreamSerializer)

    def infer_from_value_type(self, cls: type[T]) -> Serializer[T]:
        """Get the first serializer that can handle the given type or its parent classes."""
        for base in cls.mro():
            if item := self._by_value_type.get(base):
                return item
        msg = f"No value {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)

    def infer_from_stream_type(self, cls: type[T]) -> StreamSerializer[T]:
        """Get the first serializer that can handle the given type or its base classes."""
        for base in cls.mro():
            if item := self._by_stream_type.get(base):
                return item
        msg = f"No stream {self.value_description.lower()} found for {cls}."
        raise ValueError(msg)


class StorageRegistry(_BaseRegistry[str, Storage]):
    """A registry of storages."""

    value_description = "Storage"

    def __init__(
        self,
        storages: Sequence[Storage] = (),
        *,
        default: Storage | None = None,
        ignore_conflicts: bool = False,
    ) -> None:
        super().__init__(
            (default, *storages) if default else storages,
            ignore_conflicts=ignore_conflicts,
        )
        self._default = default

    @property
    def default(self) -> Storage:
        """Get the default storage."""
        if not self._default:
            msg = f"No default {self.value_description.lower()} is set."
            raise ValueError(msg)
        return self._default

    def has_default(self) -> bool:
        """Return whether a default storage is set."""
        return self._default is not None

    def get_key(self, storage: Storage) -> str:
        """Get the key for the given storage."""
        return storage.name

    @classmethod
    def can_register(cls, value: Any) -> TypeIs[Storage]:
        """Return whether the given value is a valid serializer."""
        return isinstance(value, Storage)

    @classmethod
    def merge(cls, *registries: Self, ignore_conflicts: bool = False) -> Self:
        """Merge the given registries into a new registry."""
        new_storages: list[Storage] = []

        default = None
        for r in registries:
            storages = list(r.values())
            if r.has_default():
                if default and not ignore_conflicts:
                    msg = f"Conflicting default storages: {default!r} and {r.default!r}"
                    raise ValueError(msg)
                default = r.default
                new_storages.extend(storages[1:])
            else:
                new_storages.extend(storages)

        return cls(new_storages, ignore_conflicts=ignore_conflicts, default=default)

    @classmethod
    def from_modules(
        cls,
        *modules: ModuleType | str,
        ignore_conflicts: bool = False,
        default: str | Storage | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a registry from a module."""
        if isinstance(default, str):
            default = _get_module_attr(default)
            if not cls.can_register(default):
                msg = f"Declared default storage {default!r} is not a valid storage."
                raise ValueError(msg)
        return super().from_modules(
            *modules,
            ignore_conflicts=ignore_conflicts,
            default=default,
            **kwargs,
        )


def _get_module_attr(name: str) -> Any:
    module_name, attr_name = name.rsplit(".", 1)
    module = import_module(module_name)
    return getattr(module, attr_name)


EMPTY_MODEL_REGISTRY = ModelRegistry()
"""An empty registry of models."""
EMPTY_SERIALIZER_REGISTRY = SerializerRegistry()
"""An empty registry of serializers."""
EMPTY_STORAGE_REGISTRY = StorageRegistry()
"""An empty registry of storages."""


@frozenclass
class RegistryCollection:
    """A collection of registries."""

    models: ModelRegistry = EMPTY_MODEL_REGISTRY
    """A registry of models."""
    serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY
    """A registry of serializers."""
    storages: StorageRegistry = EMPTY_STORAGE_REGISTRY
    """A registry of storages."""

    @classmethod
    def merge(
        cls,
        *others: RegistryCollection,
        models: ModelRegistry = EMPTY_MODEL_REGISTRY,
        serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY,
        storages: StorageRegistry = EMPTY_STORAGE_REGISTRY,
        ignore_conflicts: bool = False,
    ) -> Self:
        """Return a new collection of registries that merges this one with the given ones."""
        return cls(
            models=ModelRegistry.merge(
                models,
                *(r.models for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            serializers=SerializerRegistry.merge(
                serializers,
                *(r.serializers for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
            storages=StorageRegistry.merge(
                storages,
                *(r.storages for r in others),
                ignore_conflicts=ignore_conflicts,
            ),
        )
