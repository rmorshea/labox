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
from typing import cast
from uuid import UUID

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

from lakery.common.utils import UNDEFINED
from lakery.core._registry import Registry

if TYPE_CHECKING:
    from lakery.core.serializer import SerializerRegistry
    from lakery.core.serializer import StreamDump
    from lakery.core.serializer import StreamSerializer
    from lakery.core.serializer import ValueDump
    from lakery.core.serializer import ValueSerializer
    from lakery.core.storage import Storage


T = TypeVar("T")
D = TypeVar("D", bound="StorageModelDump", default="StorageModelDump")


class StorageModel(Generic[D], abc.ABC):
    """A model that can be stored and loaded."""

    storage_model_uuid: ClassVar[LiteralString]
    """A unique ID to identify this model class."""

    @abc.abstractmethod
    async def storage_model_dump(self, serializers: SerializerRegistry, /) -> D:
        """Turn the given model into its serialized components."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    async def storage_model_load(cls, dump: D, serializers: SerializerRegistry, /) -> Self:
        """Turn the given serialized components back into a model."""
        raise NotImplementedError


StorageModelDump: TypeAlias = Mapping[str, "StorageDump"]
"""A mapping of string identifiers to serialized components and their storages."""


class StorageValueDump(TypedDict):
    """Spec for how to store a serialized value."""

    value_dump: ValueDump
    storage: Storage | None


class StorageStreamDump(TypedDict):
    """Spec for how to store a serialized stream."""

    stream_dump: StreamDump
    storage: Storage | None


StorageDump = StorageValueDump | StorageStreamDump
"""Spec for how to store a serialized value or stream."""


@dataclass(frozen=True)
class ValueModel(Generic[T], StorageModel[Mapping[str, StorageValueDump]]):
    """Models a single value."""

    storage_model_uuid = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: ValueSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    async def storage_model_dump(
        self, serializers: SerializerRegistry
    ) -> Mapping[str, StorageValueDump]:
        """Turn the given model into its serialized components."""
        serializer = self.serializer or serializers.infer_from_value_type(type(self.value))
        value_dump = serializer.dump_value(self.value)
        return {"": {"value_dump": value_dump, "storage": self.storage}}

    @classmethod
    async def storage_model_load(
        cls, dump: Mapping[str, StorageValueDump], serializers: SerializerRegistry
    ) -> Self:
        """Turn the given serialized components back into a model."""
        storage_dump = dump[""]
        value_dump = storage_dump["value_dump"]
        serializer = cast("ValueSerializer", serializers[value_dump["serializer_name"]])
        value = serializer.load_value(value_dump)
        return cls(value=value, serializer=serializer, storage=storage_dump["storage"])


@dataclass(frozen=True)
class StreamModel(Generic[T], StorageModel[Mapping[str, StorageStreamDump]]):
    """Models a single stream."""

    storage_model_uuid = "e80e8707ffdd4785b95b30247fa4398c"

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""
    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    async def storage_model_dump(
        self, serializers: SerializerRegistry
    ) -> Mapping[str, StorageStreamDump]:
        """Turn the given model into its serialized components."""
        if self.serializer is None:
            stream_iter = aiter(self.stream)
            first_value = await anext(stream_iter, UNDEFINED)
            if first_value is UNDEFINED:
                msg = "Cannot infer stream serializer from empty stream."
                raise ValueError(msg)

            serializer = serializers.infer_from_stream_type(type(first_value))

            async def wrap_stream():
                yield first_value
                async for remaining_value in stream_iter:
                    yield remaining_value

            stream_dump = serializer.dump_stream(wrap_stream())
        else:
            stream_dump = self.serializer.dump_stream(self.stream)

        return {"": {"stream_dump": stream_dump, "storage": self.storage}}

    @classmethod
    async def storage_model_load(
        cls, dump: Mapping[str, StorageStreamDump], serializers: SerializerRegistry
    ) -> Self:
        """Turn the given serialized components back into a model."""
        storage_dump = dump[""]
        stream_dump = storage_dump["stream_dump"]
        serializer = cast("StreamSerializer", serializers[stream_dump["serializer_name"]])
        stream = serializer.load_stream(stream_dump)
        return cls(stream=stream, serializer=serializer, storage=storage_dump["storage"])


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
