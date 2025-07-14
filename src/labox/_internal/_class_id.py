from typing import TypeVar
from typing import cast
from uuid import uuid4
from warnings import warn

from labox._internal._utils import full_class_name

S = TypeVar("S", bound=str)


def validate_class_id(
    cls: type,
    id_str: S | None,
    *,
    warn_with_stacklevel: int | None = None,
) -> S | None:
    """Validate the ID string and pad it to 16 bytes."""
    if id_str is None:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None
    if len(id_str) < 8 or len(id_str) > 32:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None
    try:
        return pad_uuid_str(id_str)
    except ValueError:
        _id_warning_or_error(cls, id_str, warn_with_stacklevel)
        return None


def pad_uuid_str(id_str: S) -> S:
    """Pad the ID string to 16 bytes, if necessary."""
    byte_arr = bytes.fromhex(id_str)
    if len(byte_arr) < 16:
        # Pad the byte array to 16 bytes
        byte_arr = byte_arr.ljust(16, b"\0")
    return cast("S", byte_arr.hex())


def _id_warning_or_error(cls: type, given: str | None, warn_with_stacklevel: int | None) -> None:
    msg = (
        f"{given!r} is not a valid ID for {full_class_name(cls)}. Expected 8-32 character "
        f"hexadecimal string. Try using {uuid4().hex!r} instead."
    )
    if warn_with_stacklevel is not None:
        warn(msg, UserWarning, stacklevel=3 + warn_with_stacklevel)
        return
    raise ValueError(msg)
