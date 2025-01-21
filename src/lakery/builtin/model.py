from __future__ import annotations

from collections.abc import AsyncIterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import Self
from typing import TypeVar

from lakery.core.model import BaseStorageModel
from lakery.core.model import Manifest
from lakery.core.model import ModelRegistry
from lakery.core.model import StreamManifest

if TYPE_CHECKING:
    from lakery.core.context import Registries
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage

T = TypeVar("T")


def get_model_registry() -> ModelRegistry:
    """Return a registry of models from this module."""
    return ModelRegistry(_MODELS)


@dataclass(frozen=True)
class Singular(Generic[T], BaseStorageModel[Mapping[str, Manifest]]):
    """Models a single value."""

    storage_model_id = "63b297f66dbc44bb8552f6f490cf21cb"

    value: T
    """The value."""
    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_dump(self, registries: Registries) -> Mapping[str, Manifest]:
        """Dump the model to a series of storage manifests."""
        serializer = registries.serializers.infer_from_value_type(type(self.value))
        return {"": {"value": self.value, "serializer": serializer, "storage": self.storage}}

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
class Streamed(Generic[T], BaseStorageModel[Mapping[str, StreamManifest]]):
    """Models a stream of data."""

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


_MODELS: set[type[BaseStorageModel[Any]]] = {
    Singular,
    Streamed,
}


_LOG = getLogger(__name__)
