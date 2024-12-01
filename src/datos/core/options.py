import os
from dataclasses import Field
from dataclasses import fields
from typing import Annotated
from typing import Any
from typing import Self
from typing import get_args
from typing import get_origin

from datos.utils.misc import frozenclass


@frozenclass
class Options:
    """Common configuration options."""

    datos_MAX_ARCHIVE_EXISTING_RETRIES: int = 3
    """Number of times to retry archiving an existing artifact."""

    @classmethod
    def from_env(cls) -> Self:
        """Create a new instance from environment variables."""
        kwargs = {f.name: _get_from_env(f) for f in fields(cls) if f.name in os.environ}
        return cls(**kwargs)


def _get_from_env(f: Field) -> Any:
    """Get the value of the field from the environment.

    To cast from a string to some other type `Annotate` the field with a function:

    ```python
    from typing import Annotated

    IntOrNone = Annotated[int | None, lambda s: None if s.lower() == "none" else int(s)]
    ```
    """
    anno = f.type
    if get_origin(anno) is Annotated:
        _, from_str = get_args(anno)
    elif callable(anno):
        from_str = anno
    else:
        msg = "Could not infer how to load from string - use Annotated to add a callable."
        raise TypeError(msg)
    return from_str(os.environ[f.name])
