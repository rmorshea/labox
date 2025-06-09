from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar
from typing import Unpack
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn

from lakery._internal.utils import full_class_name

if TYPE_CHECKING:
    from lakery.core.unpacker import Unpacker

T = TypeVar("T")
S = TypeVar("S", bound=str)
K = TypeVar("K")


class LongStorableConfigDict(TypedDict, total=False):
    """Configuration for a storable class with explicit names."""

    storable_class_id: LiteralString | None
    """ID of the storable class, as a 16-character hexadecimal string."""
    storable_unpacker: Unpacker | None
    """ID of the storable class, as a 16-character hexadecimal string."""


class StorableConfigDict(LongStorableConfigDict, total=False):
    """Configuration for a storable class."""

    class_id: LiteralString | None
    """An alias for `storable_class_id`."""
    unpacker: Unpacker | None
    """An alias for `storable_unpacker`."""


@dataclass(frozen=True)
class StorableConfig:
    """Configuration for a storable class."""

    class_id: UUID | None = None
    unpacker: Unpacker | None = None


class Storable:
    """Base class for storable objects."""

    storable_config: ClassVar[StorableConfig] = StorableConfig()
    """Configuration for the storable class."""

    def __init_subclass__(cls, **kwargs: Unpack[StorableConfigDict]) -> None:
        cfg = normalize_storable_config_dict(kwargs)
        if (class_id := cfg.get("storable_class_id")) is not None:
            class_id = _validate_id(cls, class_id, warn_with_stacklevel=2)
            if class_id is not None:
                class_id = UUID(class_id)
        unpacker = cfg.get("storable_unpacker", cls.storable_config.unpacker)
        cls.storable_config = StorableConfig(class_id=class_id, unpacker=unpacker)


def normalize_storable_config_dict(cfg: StorableConfigDict) -> LongStorableConfigDict:
    """Normalize a StorableConfigDict to an ExplicitStorableConfigDict."""
    normalized: LongStorableConfigDict = {}
    for long_name in LongStorableConfigDict.__annotations__:
        if long_name in cfg:
            normalized[long_name] = cfg[long_name]
        else:
            short_name = _to_short_config_name(long_name)
            if short_name in cfg:
                normalized[long_name] = cfg[short_name]
    return normalized


def _to_short_config_name(long_name: str) -> str:
    """Convert an explicit name to a short name."""
    return long_name.removeprefix("storable_")


def _validate_id(
    cls: type,
    id_str: S,
    *,
    warn_with_stacklevel: int | None = None,
) -> S | None:
    """Validate the ID string and pad it to 16 bytes."""
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
