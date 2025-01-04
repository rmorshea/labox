from __future__ import annotations

from collections.abc import AsyncIterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import Required
from typing import Self
from typing import TypeAlias
from typing import TypedDict
from typing import TypeVar
from uuid import UUID

if TYPE_CHECKING:
    from lakery.core.serializer import StreamSerializer
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import Storage


ModelDump: TypeAlias = "Mapping[str, StorageValueSpec | StorageStreamSpec]"
"""A mapping of string identifiers to serialized components and their storages."""

T = TypeVar("T")
D = TypeVar("D", bound=ModelDump)


class StorageModel(Protocol[D]):
    """A model that can be stored and loaded."""

    storage_model_id: ClassVar[LiteralString]
    """A globally unique key to identify this model class."""
    storage_model_version: ClassVar[int]
    """The version of the storage model."""

    def storage_model_dump(self) -> D:
        """Turn the given model into its serialized components."""
        raise NotImplementedError

    @classmethod
    def storage_model_load(cls, dump: D, version: int) -> Self:
        """Turn the given serialized components back into a model."""
        raise NotImplementedError


class StorageValueSpec(Generic[T], TypedDict, total=False):
    """A value storage specification."""

    value: Required[T]
    serializer: ValueSerializer[T]
    storage: Storage


class _BaseStorageStreamSpec(Generic[T], TypedDict, total=False):
    stream: Required[AsyncIterable[T]]
    storage: Storage


class _KnownStorageStreamSpec(_BaseStorageStreamSpec[T]):
    serializer: StreamSerializer[T]


class _InferStorageStreamSpec(_BaseStorageStreamSpec[T]):
    type: type[T]


StorageStreamSpec = _KnownStorageStreamSpec | _InferStorageStreamSpec
"""A stream storage specification."""


@dataclass(frozen=True)
class ValueModel(Generic[T], StorageModel[Mapping[str, StorageValueSpec]]):
    """Models a single value."""

    storage_model_id = "63b297f66dbc44bb8552f6f490cf21cb"
    storage_model_version = 1

    value: T
    """The value."""
    serializer: ValueSerializer[T] | None = None
    """The serializer for the value."""
    storage: Storage | None = None
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
    def storage_model_load(cls, dump: Mapping[str, StorageValueSpec], version: int) -> Self:
        """Turn the given serialized components back into a model."""
        if version > 1:
            msg = f"Stream model version {version} is not supported."
            raise ValueError(msg)
        spec = dump[""]
        return cls(
            value=spec["value"],
            serializer=spec.get("serializer"),
            storage=spec.get("storage"),
        )


@dataclass(frozen=True)
class StreamModel(Generic[T], StorageModel[Mapping[str, StorageStreamSpec]]):
    """Models a single stream."""

    storage_model_id = "e80e8707ffdd4785b95b30247fa4398c"
    storage_model_version = 1

    stream: AsyncIterable[T]
    """The stream."""
    serializer: StreamSerializer[T] | type[T]
    """The serializer for the stream or the type of the stream."""
    storage: Storage | None = None
    """The storage for the stream."""

    def storage_model_dump(self) -> Mapping[str, StorageStreamSpec]:
        """Turn the given model into its serialized components."""
        spec: StorageStreamSpec
        if isinstance(self.serializer, type):
            spec = {"stream": self.stream, "type": self.serializer}
        else:
            spec = {"stream": self.stream, "serializer": self.serializer}
        if self.storage:
            spec["storage"] = self.storage
        return {"": spec}

    @classmethod
    def storage_model_load(cls, dump: Mapping[str, StorageStreamSpec], version: int) -> Self:
        """Turn the given serialized components back into a model."""
        if version > 1:
            msg = f"Stream model version {version} is not supported."
            raise ValueError(msg)
        spec = dump[""]
        if (serializer := spec.get("serializer")) is None:
            msg = "Stream serializer must be specified."
            raise ValueError(msg)
        return cls(
            stream=spec["stream"],
            serializer=serializer,
            storage=spec.get("storage"),
        )


M = TypeVar("M", bound=StorageModel)


class ModelRegistry:
    """A registry of storage models."""

    def __init__(
        self,
        types: Sequence[type[StorageModel]],
        *,
        include_core_models: bool = True,
    ) -> None:
        if include_core_models:
            types = (ValueModel, StreamModel, *types)
        self._by_id: dict[UUID, type[StorageModel]] = {UUID(t.storage_model_id): t for t in types}

    def get_by_id(self, model_id: UUID) -> type[StorageModel]:
        """Get the model with the given ID."""
        try:
            return self._by_id[model_id]
        except KeyError:
            msg = f"Model {model_id!r} is not registered."
            raise ValueError(msg) from None

    def add(self, model: type[M]) -> type[M]:
        """Register the given model."""
        if (
            model.storage_model_id in self._by_id
            and self._by_id[model.storage_model_id] is not model
        ):
            msg = f"Model {model.storage_model_id!r} is already registered."
            raise ValueError(msg)
        self._by_id[UUID(model.storage_model_id)] = model
        return model
