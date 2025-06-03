from collections.abc import Callable
from typing import LiteralString
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn
from weakref import WeakKeyDictionary

from lakery._internal.utils import full_class_name

T = TypeVar("T")
S = TypeVar("S", bound=str)


# We do not automatically include these in the registry because this is only
# populated when a model's module is imported. To ensure consistent behavior
# we require models to be explicitly added to a registry.
_MODEL_ID_STR_BY_TYPE: WeakKeyDictionary[type, str] = WeakKeyDictionary()


def has_model_id(model_type: type[T]) -> bool:
    """Check if a model type has a registered model ID."""
    return model_type in _MODEL_ID_STR_BY_TYPE


def get_model_id(model_type: type[T]) -> UUID:
    """Return the ID of a model type."""
    try:
        id_str = _MODEL_ID_STR_BY_TYPE[model_type]
    except KeyError:
        msg = (
            f"{full_class_name(model_type)} does not have a model ID - "
            "did you use the model_id(...) decorator?"
        )
        raise ValueError(msg) from None
    _validate_id(model_type, id_str)
    return UUID(id_str)


def model_id(model_id: LiteralString, /) -> Callable[[type[T]], type[T]]:
    """Return a decorator to register a model with a specific ID.

    The ID must be a valid 8-16 character hexadecimal string.
    """

    def decorator(cls: type[T]) -> type[T]:
        _validate_id(cls, model_id, warn_with_stacklevel=2)
        _MODEL_ID_STR_BY_TYPE[cls] = model_id
        return cls

    return decorator


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
