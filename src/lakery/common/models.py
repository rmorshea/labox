from __future__ import annotations

from dataclasses import field
from logging import getLogger
from typing import TYPE_CHECKING
from typing import Generic
from typing import Self
from typing import TypeVar

from annotated_types import KW_ONLY

from lakery.common.utils import frozenclass
from lakery.core.model import BaseStorageModel
from lakery.core.model import Manifest
from lakery.core.model import ManifestMap
from lakery.core.model import StreamManifest

if TYPE_CHECKING:
    from collections.abc import AsyncIterable
    from collections.abc import Mapping

    from lakery.core.context import Registries
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage

__all__ = (
    "Singular",
    "Streamed",
)

T = TypeVar("T")


@frozenclass(kw_only=False)
class Singular(
    Generic[T],
    BaseStorageModel,
    storage_model_id="63b297f66dbc44bb8552f6f490cf21cb",
):
    """Models a single value."""

    value: T
    """The value."""

    _ = KW_ONLY

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
        manifests: ManifestMap,
        _registries: Registries,
    ) -> Self:
        """Load the model from a series of storage manifests."""
        man = manifests[""]
        assert "value" in man, f"Missing value in manifest {man}"  # noqa: S101
        return cls(value=man["value"], serializer=man["serializer"], storage=man["storage"])


@frozenclass(kw_only=False)
class Streamed(
    Generic[T],
    BaseStorageModel,
    storage_model_id="e80e8707ffdd4785b95b30247fa4398c",
):
    """Models a stream of data."""

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""

    _ = KW_ONLY

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
        manifests: ManifestMap,
        _registries: Registries,
    ) -> Self:
        """Load the model from a series of storage manifests."""
        man = manifests[""]
        assert "stream" in man, f"Missing stream in manifest {man}"  # noqa: S101
        return cls(stream=man["stream"], serializer=man["serializer"], storage=man["storage"])


_LOG = getLogger(__name__)
