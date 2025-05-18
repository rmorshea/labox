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
from typing import NoReturn
from typing import Self
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
D = TypeVar("D", bound=Mapping[str, Any])


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


class BaseStorageModel(abc.ABC, Generic[D]):
    """A base class for models that can be stored and serialized."""

    _storage_model_config: ClassVar[StorageModelConfig | None] = None

    def __init_subclass__(
        cls,
        *,
        storage_model_config: StorageModelConfigDict | None,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        try:
            cls._storage_model_config = _make_frozen_conig(storage_model_config)
        except ValueError as err:
            warn(
                f"Ignoring storage model config for {full_class_name(cls)!r} - {err}",
                UserWarning,
                stacklevel=2,
            )
            cls._storage_model_config = None

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
    def storage_model_dump(self, registries: RegistryCollection, /) -> D:
        """Return a mapping of contents that describe where and how to store the model."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(
        cls,
        contents: D,
        version: int,
        registries: RegistryCollection,
        /,
    ) -> Self:
        """Reconstitute the model from a mapping of contents."""
        raise NotImplementedError


class ModeledValue(Generic[T], TypedDict):
    """Describes where and how to store a value."""

    value: T
    """The value to store."""
    serializer: Serializer[T] | None
    """The serializer to apply to the value."""
    storage: Storage | None
    """The storage to send the serialized value to."""


class ModeledValueStream(Generic[T], TypedDict):
    """Describes where and how to store a stream."""

    value_stream: AsyncIterable[T]
    """The stream of data to store."""
    serializer: StreamSerializer[T] | None
    """The serializer to apply to the stream."""
    storage: Storage | None
    """The storage to send the serialized stream to."""


AnyModeledValue = ModeledValue | ModeledValueStream
"""Any storage value."""
ModeledValueMap = Mapping[str, ModeledValue]
"""A mapping of storage values."""
ModeledValueStreamMap = Mapping[str, ModeledValueStream]
"""A mapping of storage stream values."""
AnyModeledValueMap = Mapping[str, AnyModeledValue]
"""A mapping of any storage values."""


def _make_frozen_conig(cfg: StorageModelConfigDict | None) -> StorageModelConfig | None:
    if cfg is None:
        return None

    storage_model_id = UUID(bytes=_pad_id_str(cfg["id"]))
    storage_model_ver = cfg["version"]
    if storage_model_ver < 0:
        msg = f"Storage model version must be a positive integer. Got {storage_model_ver}."
        raise ValueError(msg) from None

    return StorageModelConfig(id=storage_model_id, version=cfg["version"])


def _pad_id_str(id_str: str) -> bytes:
    """Pad the ID string to 16 characters."""
    if len(id_str) < 8 or len(id_str) > 16:
        _raise_storage_model_id_error(id_str)
    try:
        byte_arr = bytes.fromhex(id_str)
    except ValueError:
        _raise_storage_model_id_error(id_str)
    return byte_arr.ljust(16, b"\0")


def _raise_storage_model_id_error(given: str) -> NoReturn:
    suggested_id = uuid4().hex
    msg = (
        f"{given!r} is not a valid storage model ID. Expected 8-16 character hexadecimal string. "
        f"Try adding '{suggested_id!r}' to your class definition instead."
    )
    raise ValueError(msg)
