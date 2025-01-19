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
from uuid import uuid4

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
    bound=Mapping[str, "AnyValueDump"]
    | Mapping[str, "ValueDump"]
    | Mapping[str, "ValueStreamDump"],
    default="ModelDump",
)


class BaseStorageModel(Generic[S], abc.ABC):
    """A base class for models that can be stored and serialized."""

    storage_model_id: ClassVar[LiteralString]
    """A unique ID to identify this model class."""

    @abc.abstractmethod
    def storage_model_dump(self, registries: Registries, /) -> S:
        """Turn the given model into its serialized components."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(cls, spec: S, registries: Registries, /) -> Self:
        """Turn the given serialized components back into a model."""
        raise NotImplementedError


class ValueDump(Generic[T], TypedDict):
    """Spec for how to store and serialize a value."""

    value: T
    serializer: Serializer[T] | None
    storage: Storage | None


class ValueStreamDump(Generic[T], TypedDict):
    """Spec for how to store and serialize a stream."""

    value_stream: AsyncIterable[T]
    serializer: StreamSerializer[T] | None
    storage: Storage | None


AnyValueDump: TypeAlias = ValueDump | ValueStreamDump
"""A spec for how to store and serialize a value or stream."""

ModelDump: TypeAlias = Mapping[str, AnyValueDump]
"""A mapping of string identifiers to storage specs."""


@dataclass(frozen=True)
class ValueModel(Generic[T], BaseStorageModel[Mapping[str, ValueDump]]):
    """Models a single value."""

    storage_model_id = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_dump(self, _registries: Registries) -> Mapping[str, ValueDump]:
        """Turn the given model into its serialized components."""
        return {"": {"value": self.value, "serializer": self.serializer, "storage": self.storage}}

    @classmethod
    def storage_model_load(
        cls,
        spec: Mapping[str, ValueDump],
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
class StreamModel(Generic[T], BaseStorageModel[Mapping[str, ValueStreamDump]]):
    """Models a single stream."""

    storage_model_id = "e80e8707ffdd4785b95b30247fa4398c"

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""
    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    def storage_model_dump(self, _registries: Registries) -> Mapping[str, ValueStreamDump]:
        """Turn the given model into its serialized components."""
        return {
            "": {
                "value_stream": self.stream,
                "serializer": self.serializer,
                "storage": self.storage,
            }
        }

    @classmethod
    def storage_model_load(
        cls,
        spec: Mapping[str, ValueStreamDump],
        _registries: Registries,
    ) -> Self:
        """Turn the given serialized components back into a model."""
        stream_spec = spec[""]
        return cls(
            stream=stream_spec["value_stream"],
            serializer=stream_spec["serializer"],
            storage=stream_spec["storage"],
        )


M = TypeVar("M", bound=BaseStorageModel)


class ModelRegistry(Registry[UUID, type[BaseStorageModel]]):
    """A registry of storage model types."""

    value_description = "Storage model type"

    def get_key(self, model: type[BaseStorageModel]) -> UUID:
        """Get the key for the given model."""
        try:
            uuid_str = model.__dict__["storage_model_id"]
        except KeyError:
            full_class_name = f"{model.__module__}.{model.__qualname__}"
            suggested_id = uuid4().hex
            msg = (
                f"Class definition for {self.value_description.lower()} "
                f"{full_class_name} is missing a 'storage_model_id'. "
                f"You may want to add {suggested_id!r} to your class."
            )
            raise ValueError(msg) from None
        return UUID(uuid_str)

    @classmethod
    def with_core_models(cls, types: Sequence[type[BaseStorageModel]] = ()) -> ModelRegistry:
        """Create a registry with the given core models."""
        return cls((ValueModel, StreamModel, *types))
