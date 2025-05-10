from __future__ import annotations

import abc
from collections.abc import AsyncIterable
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Generic
from typing import Literal
from typing import LiteralString
from typing import Self
from typing import TypeAlias
from typing import overload
from uuid import UUID
from uuid import uuid4
from warnings import warn

from sqlalchemy.util.typing import TypedDict
from typing_extensions import TypeVar

from lakery.common.utils import frozenclass
from lakery.common.utils import full_class_name

if TYPE_CHECKING:
    from lakery.core.registries import RegistryCollection
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


T = TypeVar("T", default=Any)


class StorageModelConfigDict(TypedDict):
    """Configuration for a storage model."""

    id: LiteralString
    """The ID of the storage model type."""
    version: int
    """The version of the storage model type."""


@frozenclass
class StorageModelConfig:
    """A frozen configuration for a storage model."""

    id: UUID
    """The ID of the storage model type."""
    version: int
    """The version of the storage model type."""


class BaseStorageModel(abc.ABC):
    """A base class for models that can be stored and serialized."""

    _storage_model_config: ClassVar[StorageModelConfig | None] = None

    def __init_subclass__(
        cls, *, storage_model_config: StorageModelConfigDict | None, **kwargs: Any
    ) -> None:
        super().__init_subclass__(**kwargs)
        cls._storage_model_config = _make_frozen_conig(cls, storage_model_config)

    @overload
    @classmethod
    def storage_model_config(cls, *, allow_missing: bool) -> StorageModelConfig | None: ...

    @overload
    @classmethod
    def storage_model_config(cls, *, allow_missing: Literal[False] = ...) -> StorageModelConfig: ...

    @classmethod
    def storage_model_config(cls, *, allow_missing: bool = False) -> StorageModelConfig | None:
        """Return the storage model config for this class."""
        if cls._storage_model_config is None:
            if not allow_missing:
                msg = f"{full_class_name(cls)} has no storage model config."
                raise ValueError(msg) from None
            return None
        return cls._storage_model_config

    @abc.abstractmethod
    def storage_model_dump(self, registries: RegistryCollection, /) -> ContentMap:
        """Return a mapping of contents that describe where and how to store the model."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(
        cls, contents: ContentMap, version: int, registries: RegistryCollection, /
    ) -> Self:
        """Reconstitute the model from a mapping of contents."""
        raise NotImplementedError


class Content(Generic[T], TypedDict):
    """Describes where and how to store a value."""

    value: T
    """The value to store."""
    serializer: Serializer[T] | None
    """The serializer to apply to the value."""
    storage: Storage | None
    """The storage to send the serialized value to."""


class StreamContent(Generic[T], TypedDict):
    """Describes where and how to store a stream."""

    value_stream: AsyncIterable[T]
    """The stream of data to store."""
    serializer: StreamSerializer[T] | None
    """The serializer to apply to the stream."""
    storage: Storage | None
    """The storage to send the serialized stream to."""


AnyContent: TypeAlias = Content | StreamContent
"""A type alias for any content."""

ContentMap: TypeAlias = Mapping[str, AnyContent]
"""A type alias for a mapping of contents."""


def _make_frozen_conig(
    cls: type[BaseStorageModel],
    cfg: StorageModelConfigDict | None,
) -> StorageModelConfig | None:
    if cfg is None:
        cls._storage_model_config = None
        return None
    elif "id" not in cfg:
        suggested_id = uuid4().hex
        msg = (
            f"No storage model ID declared for {full_class_name(cls)}. You may "
            f"want to add {suggested_id!r} to your class definition instead."
        )
        warn(msg, UserWarning, stacklevel=2)
        cls._storage_model_config = None
        return None

    storage_model_id = UUID(bytes=_pad_id_str(cfg["id"]))
    storage_model_ver = cfg["version"]
    if storage_model_ver < 0:
        msg = f"Storage model version must be a positive integer. Got {storage_model_ver}."
        raise ValueError(msg) from None

    return StorageModelConfig(id=storage_model_id, version=cfg["version"])


def _pad_id_str(id_str: str) -> bytes:
    """Pad the ID string to 32 characters."""
    if len(id_str) > 32:
        msg = f"ID string {id_str} is too long."
        raise ValueError(msg)
    if len(id_str) < 8:
        msg = f"ID string {id_str} is too short."
        raise ValueError(msg)
    id_byte_arr = bytearray.fromhex(id_str)
    id_byte_arr.extend(b"\0" * (32 - len(id_byte_arr)))
    return bytes(id_byte_arr)
