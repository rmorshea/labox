from __future__ import annotations

from typing import TYPE_CHECKING
from typing import LiteralString
from typing import Protocol
from typing import TypeVar

from datos.core.registry import Registry
from datos.core.schema import DataRelation

if TYPE_CHECKING:
    from datos.core.serializer import ScalarDump
    from datos.core.serializer import StreamDump


R = TypeVar("R", bound=DataRelation)


class Storage(Protocol[R]):
    """A protocol for storing and retrieving data."""

    name: LiteralString
    """The name of the storage backend."""
    types: tuple[type[R], ...]
    """The types that the serializer can handle."""
    version: int

    async def write_stream(self, relation: R, dump: StreamDump) -> R:
        """Save the given stream dump."""
        ...

    async def read_stream(self, relation: R) -> StreamDump:
        """Load the stream dump for the given ORM."""
        ...

    async def write_scalar(self, relation: R, dump: ScalarDump) -> R:
        """Save the given scalar dump."""
        ...

    async def read_scalar(self, relation: R) -> ScalarDump:
        """Load the scalar dump for the given ORM."""
        ...


class StorageRegistry(Registry[Storage]):
    """A registry of storages."""

    item_description = "Storage"

    if TYPE_CHECKING:

        def get_by_type_inference(self, cls: type[R]) -> Storage[R]: ...  # noqa: D102
