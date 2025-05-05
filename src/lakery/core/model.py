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

from lakery.common.utils import full_class_name

if TYPE_CHECKING:
    from lakery.core.registries import RegistryCollection
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage


T = TypeVar("T", default=Any)


class BaseStorageModel(abc.ABC):
    """A base class for models that can be stored and serialized."""

    _storage_model_id: ClassVar[UUID | None]

    def __init_subclass__(cls, *, storage_model_id: LiteralString | None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        if storage_model_id is None:
            cls._storage_model_id = None
            return

        try:
            cls._storage_model_id = UUID(storage_model_id)
        except TypeError:
            suggested_id = uuid4().hex
            msg = (
                f"{storage_model_id!r} is not a valid storage model ID for {full_class_name(cls)}. "
                f"You may want to add {suggested_id!r} to your class definition instead."
            )
            warn(msg, UserWarning, stacklevel=2)

    @overload
    @classmethod
    def storage_model_id(cls, *, allow_missing: bool) -> UUID | None: ...

    @overload
    @classmethod
    def storage_model_id(
        cls,
        *,
        allow_missing: Literal[False] = ...,
    ) -> UUID: ...

    @classmethod
    def storage_model_id(cls, *, allow_missing: bool = False) -> UUID | None:
        """Return a UUID that uniquely identifies this model type.

        This is used to later determine which class to reconstitute when loading data later.
        That means you should **never copy or change this** value once it's been used to
        save data.
        """
        try:
            s_id = cls._storage_model_id
        except AttributeError:
            suggested_id = uuid4().hex
            msg = (
                f"{full_class_name(cls)} is missing a valid 'storage_model_id'. "
                f"Try adding {suggested_id!r} to your class definition."
            )
            raise ValueError(msg) from None

        if s_id is None:
            if not allow_missing:
                msg = f"Abstract storage model {full_class_name(cls)} has no 'storage_model_id'."
                raise ValueError(msg) from None
            return None

        return s_id

    @abc.abstractmethod
    def storage_model_dump(self, registries: RegistryCollection, /) -> ManifestMap:
        """Return a mapping of manifests that describe where and how to store the model."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def storage_model_load(cls, manifests: ManifestMap, registries: RegistryCollection, /) -> Self:
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
