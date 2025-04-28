from __future__ import annotations

import abc
from importlib import import_module
from typing import TYPE_CHECKING
from typing import Any
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import Self
from typing import TypedDict
from typing import TypeIs
from typing import TypeVar

from lakery.core._registry import Registry

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator
    from collections.abc import AsyncIterable
    from collections.abc import Sequence
    from types import ModuleType

    from lakery.common.utils import TagMap


T = TypeVar("T")


class Storage(Generic[T], abc.ABC):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage."""

    @abc.abstractmethod
    async def put_data(
        self,
        data: bytes,
        digest: Digest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given data and return information that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    async def get_data(self, info: T, /) -> bytes:
        """Load data using the given information."""
        raise NotImplementedError

    @abc.abstractmethod
    async def put_data_stream(
        self,
        data_stream: AsyncIterable[bytes],
        get_digest: GetStreamDigest,
        tags: TagMap,
        /,
    ) -> T:
        """Save the given stream and return information that can be used to retrieve it."""
        raise NotImplementedError

    @abc.abstractmethod
    def get_data_stream(self, data: T, /) -> AsyncGenerator[bytes]:
        """Load a stream of data using the given information."""
        raise NotImplementedError

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


class Digest(TypedDict):
    """A digest describing serialized data."""

    content_encoding: str | None
    """The encoding of the data."""
    content_type: str
    """The MIME type of the data."""
    content_hash_algorithm: str
    """The algorithm used to hash the data."""
    content_hash: str
    """The hash of the data."""
    content_size: int
    """The size of the data in bytes."""


class StreamDigest(Digest):
    """A digest describing a stream of serialized data."""

    is_complete: bool
    """A flag indicating whether the stream has been read in full."""


class GetStreamDigest(Protocol):
    """A protocol for getting the digest of stream content."""

    def __call__(self, *, allow_incomplete: bool = False) -> StreamDigest:
        """Get the digest of a stream dump.

        Args:
            allow_incomplete: Whether to allow the digest to be incomplete.
               Raises an error if the digest is incomplete and this is False.

        Raises:
            ValueError: If the digest is incomplete and `allow_incomplete` is False.
        """
        ...


class StorageRegistry(Registry[str, Storage]):
    """A registry of storages."""

    value_description = "Storage"

    def __init__(
        self,
        storages: Sequence[Storage] = (),
        *,
        default: Storage | None = None,
        ignore_conflicts: bool = False,
    ) -> None:
        super().__init__(
            (default, *storages) if default else storages,
            ignore_conflicts=ignore_conflicts,
        )
        self._default = default

    @property
    def default(self) -> Storage:
        """Get the default storage."""
        if not self._default:
            msg = f"No default {self.value_description.lower()} is set."
            raise ValueError(msg)
        return self._default

    def has_default(self) -> bool:
        """Return whether a default storage is set."""
        return self._default is not None

    def get_key(self, storage: Storage) -> str:
        """Get the key for the given storage."""
        return storage.name

    @classmethod
    def can_register(cls, value: Any) -> TypeIs[Storage]:
        """Return whether the given value is a valid serializer."""
        return isinstance(value, Storage)

    @classmethod
    def merge(cls, *registries: Self, ignore_conflicts: bool = False) -> Self:
        """Merge the given registries into a new registry."""
        new_storages: list[Storage] = []

        default = None
        for r in registries:
            storages = list(r.values())
            if r.has_default():
                if default and not ignore_conflicts:
                    msg = f"Conflicting default storages: {default!r} and {r.default!r}"
                    raise ValueError(msg)
                default = r.default
                new_storages.extend(storages[1:])
            else:
                new_storages.extend(storages)

        return cls(new_storages, ignore_conflicts=ignore_conflicts, default=default)

    @classmethod
    def from_modules(
        cls,
        *modules: ModuleType | str,
        ignore_conflicts: bool = False,
        default: str | Storage | None = None,
        **kwargs: Any,
    ) -> Self:
        """Create a registry from a module."""
        if isinstance(default, str):
            default = _get_module_attr(default)
            if not cls.can_register(default):
                msg = f"Declared default storage {default!r} is not a valid storage."
                raise ValueError(msg)
        return super().from_modules(
            *modules,
            ignore_conflicts=ignore_conflicts,
            default=default,
            **kwargs,
        )


def _get_module_attr(name: str) -> Any:
    module_name, attr_name = name.rsplit(".", 1)
    module = import_module(module_name)
    return getattr(module, attr_name)
