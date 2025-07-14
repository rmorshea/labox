from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Literal
from typing import LiteralString
from typing import TypeVar
from typing import Unpack
from typing import overload
from uuid import UUID
from uuid import uuid4

from typing_extensions import TypedDict

from labox._internal._class_id import validate_class_id
from labox._internal._utils import full_class_name

if TYPE_CHECKING:
    from labox.core.unpacker import Unpacker

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
            cls._storable_class_id = validate_class_id(
                cls,
                cfg["storable_class_id"],
                warn_with_stacklevel=2,
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
