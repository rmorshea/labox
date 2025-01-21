from __future__ import annotations

import abc
from collections.abc import AsyncIterable
from collections.abc import Mapping
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
    """A UUID that uniquely identifies the model.

    This is used to later determine which class to reconstitute when loading data later.
    That means you should **never copy or change this** value once it's been used to
    save data.
    """

    @abc.abstractmethod
    def storage_model_dump(self, registries: Registries, /) -> S:
        """Return a mapping of manifests that describe where and how to store the model."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(cls, manifests: S, registries: Registries, /) -> Self:
        """Reconstitute the model from a mapping of manifests."""
        raise NotImplementedError


class Manifest(Generic[T], TypedDict):
    """Describes where and how to store a value."""

    value: T
    """The value to store."""
    serializer: Serializer[T] | None
    """The serializer to apply to the value."""
    storage: Storage | None
    """The storage to send the serialized value to."""


class StreamManifest(Generic[T], TypedDict):
    """Describes where and how to store a stream."""

    stream: AsyncIterable[T]
    """The stream of data to store."""
    serializer: StreamSerializer[T] | None
    """The serializer to apply to the stream."""
    storage: Storage | None
    """The storage to send the serialized stream to."""


AnyManifest: TypeAlias = Manifest | StreamManifest
"""A type alias for any manifest."""

ManifestMap: TypeAlias = Mapping[str, AnyManifest]
"""A type alias for a mapping of manifests."""


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
