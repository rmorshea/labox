from typing import TYPE_CHECKING


class NoStorageData(Exception):
    """Raised when a storage is unable to find a value or stream."""


class NotRegistered(KeyError):
    """Raised when a registry does not have an item."""

    if TYPE_CHECKING:

        def __init__(self, msg: str) -> None: ...

    def __repr__(self) -> str:
        return self.args[0]
