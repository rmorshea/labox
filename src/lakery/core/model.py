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
from typing import Self
from typing import TypeAlias
from uuid import UUID

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from lakery.core.context import Registries
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


T = TypeVar("T")
S = TypeVar(
    "S",
    bound=Mapping[str, "StorageSpec"]
    | Mapping[str, "ValueStorageSpec"]
    | Mapping[str, "StreamStorageSpec"],
    default="StorageSpecMap",
)


class StorageModel(Generic[S], abc.ABC):
    """A model that can be stored and loaded."""

    storage_model_uuid: ClassVar[LiteralString]
    """A unique ID to identify this model class."""

    @abc.abstractmethod
    def storage_model_to_spec(self, registries: Registries, /) -> S:
        """Turn the given model into its serialized components."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_from_spec(cls, spec: S, registries: Registries, /) -> Self:
        """Turn the given serialized components back into a model."""
        raise NotImplementedError


class ValueStorageSpec(Generic[T], TypedDict):
    """Spec for how to store and serialize a value."""

    value: T
    serializer: Serializer[T] | None
    storage: Storage | None


class StreamStorageSpec(Generic[T], TypedDict):
    """Spec for how to store and serialize a stream."""

    stream: AsyncIterable[T]
    serializer: StreamSerializer[T] | None
    storage: Storage | None


StorageSpec: TypeAlias = ValueStorageSpec | StreamStorageSpec
"""A spec for how to store and serialize a value or stream."""

StorageSpecMap: TypeAlias = Mapping[str, StorageSpec]
"""A mapping of string identifiers to storage specs."""


@dataclass(frozen=True)
class ValueModel(Generic[T], StorageModel[Mapping[str, ValueStorageSpec]]):
    """Models a single value."""

    storage_model_uuid = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_to_spec(self, _registries: Registries) -> Mapping[str, ValueStorageSpec]:
        """Turn the given model into its serialized components."""
        return {"": {"value": self.value, "serializer": self.serializer, "storage": self.storage}}

    @classmethod
    def storage_model_from_spec(
        cls,
        spec: Mapping[str, ValueStorageSpec],
        _registries: Registries,
    ) -> Self:
        """Turn the given serialized components back into a model."""
        value_spec = spec[""]
        return cls(
            value=value_spec["value"],
            serializer=value_spec["serializer"],
            storage=value_spec["storage"],
        )


@dataclass(frozen=True)
class StreamModel(Generic[T], StorageModel[Mapping[str, StreamStorageSpec]]):
    """Models a single stream."""

    storage_model_uuid = "e80e8707ffdd4785b95b30247fa4398c"

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""
    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    def storage_model_to_spec(self, _registries: Registries) -> Mapping[str, StreamStorageSpec]:
        """Turn the given model into its serialized components."""
        return {"": {"stream": self.stream, "serializer": self.serializer, "storage": self.storage}}

    @classmethod
    def storage_model_from_spec(
        cls,
        spec: Mapping[str, StreamStorageSpec],
        _registries: Registries,
    ) -> Self:
        """Turn the given serialized components back into a model."""
        stream_spec = spec[""]
        return cls(
            stream=stream_spec["stream"],
            serializer=stream_spec["serializer"],
            storage=stream_spec["storage"],
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
