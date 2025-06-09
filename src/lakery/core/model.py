from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn

from lakery._internal.utils import full_class_name

if TYPE_CHECKING:
    from lakery.core.unpacker import Unpacker

T = TypeVar("T")
S = TypeVar("S", bound=str)


class StorableConfigDict(TypedDict, total=False):
    """Configuration for a storable class."""

    class_id: LiteralString | None
    unpacker: Unpacker | None


@dataclass(frozen=True)
class StorableConfig:
    """Configuration for a storable class."""

    class_id: UUID | None = None
    unpacker: Unpacker | None = None


class Storable:
    """Base class for storable objects."""

    storable_config: ClassVar[StorableConfig] = StorableConfig()
    """Configuration for the storable class."""

    def __init_subclass__(cls, storable_config: StorableConfigDict) -> None:
        class_id = _validate_id(cls, storable_config.get("class_id"))
        if class_id is not None:
            class_id = UUID(class_id)
        unpacker = storable_config.get("unpacker", cls.storable_config.unpacker)
        cls.storable_config = StorableConfig(class_id=class_id, unpacker=unpacker)


def _validate_id(
    cls: type,
    id_str: S | None,
    *,
    warn_with_stacklevel: int | None = None,
) -> S | None:
    """Validate the ID string and pad it to 16 bytes."""
    if id_str is None:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None
    if len(id_str) < 8 or len(id_str) > 16:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None
    try:
        byte_arr = bytes.fromhex(id_str)
    except ValueError:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None
    return cast("S", byte_arr.ljust(16, b"\0").decode("ascii"))


def _id_warning_or_error(cls: type, given: str | None, warn_with_stacklevel: int | None) -> None:
    msg = (
        f"{given!r} is not a valid ID for {full_class_name(cls)}. Expected 8-16 character "
        f"hexadecimal string. Try using '{uuid4().hex!r}' instead."
    )
    if warn_with_stacklevel is not None:
        warn(msg, UserWarning, stacklevel=3 + warn_with_stacklevel)
        return
    raise ValueError(msg)
