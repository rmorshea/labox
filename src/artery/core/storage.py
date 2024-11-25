from __future__ import annotations

from typing import TYPE_CHECKING
from typing import LiteralString
from typing import Protocol

from artery.core.registry import Registry

if TYPE_CHECKING:
    from artery.core.schema import Record
    from artery.core.serializer import ScalarDump


class Storage(Protocol):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage backend."""

    async def save_scalar(self, record: Record, dump: ScalarDump) -> None:
        """Save the given scalar dump."""
        ...


class StorageRegistry(Registry[Storage]):
    """A registry of storages"""

    item_description = "Storage"
