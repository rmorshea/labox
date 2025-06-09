from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import LiteralString
from typing import TypedDict
from typing import TypeVar
from typing import cast
from typing import overload
from uuid import UUID
from uuid import uuid4
from warnings import warn
from weakref import WeakKeyDictionary

from lakery._internal.utils import full_class_name

if TYPE_CHECKING:
    from collections.abc import Callable

    from lakery.core.unpacker import Unpacker

T = TypeVar("T")
S = TypeVar("S", bound=str)


@overload
def set_model(
    cls: type[T],
    /,
    *,
    id: LiteralString,
    unpacker: Unpacker[T] | None = None,
) -> type[T]: ...


@overload
def set_model(
    *,
    id: LiteralString,
    unpacker: Unpacker[T] | None = None,
) -> Callable[[type[T]], type[T]]: ...


def set_model(
    cls: type[T] | None = None,
    /,
    *,
    id: LiteralString,  # noqa: A002
    unpacker: Unpacker[T] | None = None,
) -> type[T] | Callable[[type[T]], type[T]]:
    """Return a decorator to register a model with a specific ID.

    The ID must be a valid 8-16 character hexadecimal string.
    """
    model_id = id
    del id

    def decorator(cls: type[T]) -> type[T]:
        _validate_id(cls, model_id, warn_with_stacklevel=2)
        _MODEL_INFO_STR_BY_TYPE[cls] = {"model_id": model_id, "unpacker": unpacker}
        return cls

    return decorator if cls is None else decorator(cls)


def has_model_info(cls: type[T]) -> bool:
    """Check if a model type has a registered model ID."""
    return cls in _MODEL_INFO_STR_BY_TYPE


def get_model_info(cls: type[T]) -> ModelInfo:
    """Return the ID of a model type."""
    try:
        info = _MODEL_INFO_STR_BY_TYPE[cls]
    except KeyError:
        msg = (
            f"{full_class_name(cls)} does not have a model ID - "
            "did you use the model_id(...) decorator?"
        )
        raise ValueError(msg) from None

    _validate_id(cls, info["model_id"])

    return ModelInfo(
        model_id=UUID(info["model_id"]),
        unpacker=info["unpacker"],
    )


@dataclass(frozen=True)
class ModelInfo:
    """Metadata about a model type."""

    model_id: UUID
    """The model ID as an 8-16 character hexadecimal string."""
    unpacker: Unpacker[Any] | None
    """The unpacker used to decompose the model into its constituent parts."""


class _ModelInfo(TypedDict):
    """Metadata about a model type."""

    model_id: str
    unpacker: Unpacker[Any] | None


# We do not automatically include these in the registry because this is only
# populated when a model's module is imported. To ensure consistent behavior
# we require models to be explicitly added to a registry.
_MODEL_INFO_STR_BY_TYPE: WeakKeyDictionary[type, _ModelInfo] = WeakKeyDictionary()


def _validate_id(
    cls: type,
    id_str: S | None,
    *,
    warn_with_stacklevel: int | None = None,
) -> S | None:
    """Validate the ID string and pad it to 16 bytes."""
    if id_str is None:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return id_str
    if len(id_str) < 8 or len(id_str) > 16:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return id_str
    try:
        byte_arr = bytes.fromhex(id_str)
    except ValueError:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return id_str
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
