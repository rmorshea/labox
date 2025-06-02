from typing import ClassVar
from typing import LiteralString
from typing import TypeVar
from typing import cast
from uuid import UUID
from uuid import uuid4
from warnings import warn

S = TypeVar("S", bound=str)


class BaseModel:
    """Base class for models in Lakery."""

    _model_class_id: ClassVar[LiteralString | None] = None
    """A unique identifier for the model class, used to load the class from storage."""

    def __init_subclass__(cls, model_class_id: LiteralString | None, **kwargs) -> None:
        cls._model_class_id = _validate_model_id(model_class_id, warn_with_stacklevel=2)
        super().__init_subclass__(**kwargs)

    @classmethod
    def model_class_id(cls) -> UUID:
        """Return the unique identifier for the model class."""
        _validate_model_id(cls._model_class_id)
        return UUID(cls._model_class_id)


def _validate_model_id(
    id_str: S | None,
    *,
    warn_with_stacklevel: int | None = None,
) -> S | None:
    """Validate the storage scheme ID string and pad it to 16 bytes."""
    if id_str is None:
        _model_id_warning_or_error(id_str, warn_with_stacklevel)
        return id_str
    if len(id_str) < 8 or len(id_str) > 16:
        _model_id_warning_or_error(id_str, warn_with_stacklevel)
        return id_str
    try:
        byte_arr = bytes.fromhex(id_str)
    except ValueError:
        _model_id_warning_or_error(id_str, warn_with_stacklevel)
        return id_str
    return cast("S", byte_arr.ljust(16, b"\0").decode("ascii"))


def _model_id_warning_or_error(given: str | None, warn_with_stacklevel: int | None) -> None:
    msg = (
        f"{given!r} is not a valid storage scheme ID. Expected 8-16 character "
        f"hexadecimal string. Try using '{uuid4().hex!r}' instead."
    )
    if warn_with_stacklevel is not None:
        warn(msg, UserWarning, stacklevel=3 + warn_with_stacklevel)
        return
    raise ValueError(msg)
