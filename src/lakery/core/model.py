from __future__ import annotations

import abc
from collections.abc import AsyncIterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import Required
from typing import Self
from typing import TypeAlias
from typing import TypedDict
from uuid import UUID

from typing_extensions import TypeVar

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from lakery.core.serializer import StreamSerializer
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import Storage


StorageSpec: TypeAlias = "StorageValueSpec | StorageStreamSpec"

ModelDump: TypeAlias = (
    Mapping[str, StorageSpec] | Mapping[str, "StorageValueSpec"] | Mapping[str, "StorageStreamSpec"]
)
"""A mapping of string identifiers to serialized components and their storages."""

T = TypeVar("T")
D = TypeVar("D", bound=ModelDump)


class StorageModel(Generic[D], abc.ABC):
    """A model that can be stored and loaded."""

    storage_model_uuid: ClassVar[LiteralString]
    """A unique ID to identify this model class."""

    @abc.abstractmethod
    def storage_model_dump(self) -> D:
        """Turn the given model into its serialized components."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(cls, dump: D) -> Self:
        """Turn the given serialized components back into a model."""
        raise NotImplementedError


class StorageValueSpec(Generic[T], TypedDict, total=False):
    """A value storage specification."""

    value: Required[T]
    serializer: ValueSerializer[T]
    storage: Storage


class StorageStreamSpec(Generic[T], TypedDict, total=False):
    """A stream storage specification."""

    stream: Required[AsyncIterable[T]]
    serializer: StreamSerializer[T]
    storage: Storage


@dataclass(frozen=True)
class ValueModel(Generic[T], StorageModel[Mapping[str, StorageValueSpec]]):
    """Models a single value."""

    storage_model_uuid = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: ValueSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_dump(self) -> Mapping[str, StorageValueSpec]:
        """Turn the given model into its serialized components."""
        spec: StorageValueSpec = {"value": self.value}
        if self.serializer:
            spec["serializer"] = self.serializer
        if self.storage:
            spec["storage"] = self.storage
        return {"": spec}

    @classmethod
    def storage_model_load(cls, dump: Mapping[str, StorageValueSpec]) -> Self:
        """Turn the given serialized components back into a model."""
        spec = dump[""]
        return cls(
            value=spec["value"],
            serializer=spec.get("serializer"),
            storage=spec.get("storage"),
        )


@dataclass(frozen=True)
class StreamModel(Generic[T], StorageModel[Mapping[str, StorageStreamSpec]]):
    """Models a single stream."""

    storage_model_uuid = "e80e8707ffdd4785b95b30247fa4398c"

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""
    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    def storage_model_dump(self) -> Mapping[str, StorageStreamSpec]:
        """Turn the given model into its serialized components."""
        spec: StorageStreamSpec = {"stream": self.stream}
        if self.serializer:
            spec["serializer"] = self.serializer
        if self.storage:
            spec["storage"] = self.storage
        return {"": spec}

    @classmethod
    def storage_model_load(cls, dump: Mapping[str, StorageStreamSpec]) -> Self:
        """Turn the given serialized components back into a model."""
        spec = dump[""]
        if (serializer := spec.get("serializer")) is None:
            msg = "Stream serializer must be specified."
            raise AssertionError(msg)
        return cls(
            stream=spec["stream"],
            serializer=serializer,
            storage=spec.get("storage"),
        )


M = TypeVar("M", bound=StorageModel)


class ModelRegistry(Registry[UUID, type[StorageModel]]):
    """A registry of storage model types."""

    value_description = "Storage model type"

    def get_key(self, model: type[StorageModel]) -> UUID:
        """Get the key for the given model."""
        return UUID(model.storage_model_uuid)

    @classmethod
    def with_core_models(cls, types: Sequence[type[StorageModel]] = ()) -> ModelRegistry:
        """Create a registry with the given core models."""
        return cls((ValueModel, StreamModel, *types))
