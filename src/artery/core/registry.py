from collections import Counter
from collections.abc import Sequence
from typing import ClassVar
from typing import Generic
from typing import LiteralString
from typing import Protocol
from typing import TypeVar


class RegistryItem(Protocol):
    """A named item in a registry."""

    name: LiteralString


T = TypeVar("T", bound=RegistryItem)


class Registry(Generic[T]):
    """A registry of named items."""

    item_description: ClassVar[str]
    """A description for the type of item"""

    def __init__(self, items: Sequence[T]) -> None:
        if not items:
            msg = "At least one item must be registered."
            raise ValueError(msg)

        names = Counter(s.name for s in self.items)
        if conflicts := {n for n, c in names.items() if c > 1}:
            msg = f"Conflicting {self.item_description.lower()} names: {conflicts}"
            raise ValueError(msg)

        self.by_name = {s.name: s for s in self.items}
        self.default = items[0]
        self.items = items

    def check_registered(self, item: T) -> None:
        """Ensure that the given serializer is registered - raises a ValueError if not."""
        if (existing := self.by_name.get(item.name)) is not item:
            if existing:
                msg = f"{self.item_description} {item.name} is registered as {existing} not {item}."
                raise ValueError(msg)
            msg = f"{self.item_description} {item.name} is not registered."
            raise ValueError(msg)
