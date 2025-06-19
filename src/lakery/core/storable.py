from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Literal
from typing import LiteralString
from typing import TypeVar
from typing import Unpack
from typing import cast
from typing import overload
from uuid import UUID
from uuid import uuid4
from warnings import warn

from typing_extensions import TypedDict

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

    class_id: UUID
    unpacker: Unpacker


class Storable:
    """Base class for storable objects."""

    _storable_class_id: ClassVar[str | None] = None
    _storable_unpacker: ClassVar[Unpacker | None] = None

    def __init_subclass__(cls, **kwargs: Unpack[StorableConfigDict]) -> None:
        cfg = normalize_storable_config_dict(kwargs)

        if "storable_class_id" in cfg:
            cls._storable_class_id = _validate_id(
                cls, cfg["storable_class_id"], warn_with_stacklevel=2
            )
        else:
            cls._storable_class_id = None

        if (unpacker := cfg.get("storable_unpacker")) is not None:
            cls._storable_unpacker = unpacker

    @overload
    @classmethod
    def get_storable_config(cls, *, allow_none: bool) -> StorableConfig | None: ...

    @overload
    @classmethod
    def get_storable_config(cls, *, allow_none: Literal[False] = ...) -> StorableConfig: ...

    @classmethod
    def get_storable_config(cls, *, allow_none: bool = False) -> StorableConfig | None:
        """Get the configuration for this storable class."""
        if cls._storable_class_id is None:
            if allow_none:
                return None
            msg = (
                f"{full_class_name(cls)} does not have a valid storable class ID."
                f" Got {cls._storable_class_id!r}. Consider using {uuid4().hex!r}."
            )
            raise ValueError(msg)
        if cls._storable_unpacker is None:
            if allow_none:
                return None
            msg = f"{full_class_name(cls)} does not have a storable unpacker."
            raise ValueError(msg)
        return StorableConfig(
            class_id=UUID(cls._storable_class_id),
            unpacker=cls._storable_unpacker,
        )


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
    if len(id_str) < 16:
        # Pad the byte array to 16 bytes with zeroes
        byte_arr = byte_arr.ljust(16, b"\0")
    return cast("S", byte_arr.hex())


def _id_warning_or_error(cls: type, given: str | None, warn_with_stacklevel: int | None) -> None:
    msg = (
        f"{given!r} is not a valid ID for {full_class_name(cls)}. Expected 8-16 character "
        f"hexadecimal string. Try using {uuid4().hex!r} instead."
    )
    if warn_with_stacklevel is not None:
        warn(msg, UserWarning, stacklevel=3 + warn_with_stacklevel)
        return
    raise ValueError(msg)
