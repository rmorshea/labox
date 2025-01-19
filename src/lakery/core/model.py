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
    bound=Mapping[str, "AnyManifest"] | Mapping[str, "Manifest"] | Mapping[str, "StreamManifest"],
    default=Mapping[str, "AnyManifest"],
)


class BaseStorageModel(Generic[S], abc.ABC):
    """A base class for models that can be stored and serialized."""

    storage_model_id: ClassVar[LiteralString]
    """A unique ID to identify this model class."""

    @abc.abstractmethod
    def storage_model_dump(self, registries: Registries, /) -> S:
        """Dump the model to a series of storage manifests."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(cls, manifests: S, registries: Registries, /) -> Self:
        """Load the model from a series of storage manifests."""
        raise NotImplementedError


class Manifest(Generic[T], TypedDict):
    """Describes where and how to store a value."""

    value: T
    serializer: Serializer[T] | None
    storage: Storage | None


class StreamManifest(Generic[T], TypedDict):
    """Describes where and how to store a stream."""

    stream: AsyncIterable[T]
    serializer: StreamSerializer[T] | None
    storage: Storage | None


AnyManifest: TypeAlias = Manifest | StreamManifest
"""A type alias for any manifest."""

ManifestMap: TypeAlias = Mapping[str, AnyManifest]
"""A type alias for a mapping of manifests."""


@dataclass(frozen=True)
class ValueModel(Generic[T], BaseStorageModel[Mapping[str, Manifest]]):
    """Models a single value."""

    storage_model_id = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_dump(self, _registries: Registries) -> Mapping[str, Manifest]:
        """Dump the model to a series of storage manifests."""
        return {"": {"value": self.value, "serializer": self.serializer, "storage": self.storage}}

    @classmethod
    def storage_model_load(
        cls,
        manifests: Mapping[str, Manifest],
        _registries: Registries,
    ) -> Self:
        """Load the model from a series of storage manifests."""
        man = manifests[""]
        return cls(value=man["value"], serializer=man["serializer"], storage=man["storage"])


@dataclass(frozen=True)
class StreamModel(Generic[T], BaseStorageModel[Mapping[str, StreamManifest]]):
    """Models a single stream."""

    storage_model_id = "e80e8707ffdd4785b95b30247fa4398c"

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""
    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    def storage_model_dump(self, _registries: Registries) -> Mapping[str, StreamManifest]:
        """Dump the model to a series of storage manifests."""
        return {
            "": {
                "stream": self.stream,
                "serializer": self.serializer,
                "storage": self.storage,
            }
        }

    @classmethod
    def storage_model_load(
        cls,
        manifests: Mapping[str, StreamManifest],
        _registries: Registries,
    ) -> Self:
        """Load the model from a series of storage manifests."""
        man = manifests[""]
        return cls(stream=man["stream"], serializer=man["serializer"], storage=man["storage"])


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
