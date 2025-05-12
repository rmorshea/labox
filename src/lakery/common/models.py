from __future__ import annotations

from dataclasses import field
from typing import TYPE_CHECKING
from typing import Generic
from typing import Self
from typing import TypeVar

from annotated_types import KW_ONLY

from lakery.common.utils import frozenclass
from lakery.core.model import BaseStorageModel
from lakery.core.model import StorageValueMap
from lakery.core.model import StorageValueStreamMap

if TYPE_CHECKING:
    from collections.abc import AsyncIterable

    from lakery.core.registries import RegistryCollection
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
    BaseStorageModel[StorageValueMap],
    Generic[T],
    storage_model_config={"id": "63b297f6", "version": 1},
):
    """Models a single value."""

    value: T
    """The value."""

    _ = KW_ONLY

    serializer: Serializer[T] | None = field(default=None, compare=False)
    """The serializer for the value."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the value."""

    def storage_model_dump(self, registries: RegistryCollection) -> StorageValueMap:
        """Dump the model to storage content."""
        serializer = registries.serializers.infer_from_value_type(type(self.value))
        return {"": {"value": self.value, "serializer": serializer, "storage": self.storage}}

    @classmethod
    def storage_model_load(
        cls, contents: StorageValueMap, _version: int, _registries: RegistryCollection
    ) -> Self:
        """Load the model from storage content."""
        cont = contents[""]
        return cls(value=cont["value"], serializer=cont["serializer"], storage=cont["storage"])


@frozenclass(kw_only=False)
class Streamed(
    BaseStorageModel[StorageValueStreamMap],
    Generic[T],
    storage_model_config={"id": "e80e8707", "version": 1},
):
    """Models a stream of data."""

    stream: AsyncIterable[T] = field(compare=False)
    """The stream."""

    _ = KW_ONLY

    serializer: StreamSerializer[T] | None = field(default=None, compare=False)
    """The serializer for the stream."""
    storage: Storage | None = field(default=None, compare=False)
    """The storage for the stream."""

    def storage_model_dump(self, _registries: RegistryCollection) -> StorageValueStreamMap:
        """Dump the model to storage content."""
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
        contents: StorageValueStreamMap,
        _version: int,
        _registries: RegistryCollection,
    ) -> Self:
        """Load the model from storage content."""
        cont = contents[""]
        assert "value_stream" in cont, f"Missing stream in content {cont}"  # noqa: S101
        return cls(
            stream=cont["value_stream"],
            serializer=cont["serializer"],
            storage=cont["storage"],
        )
