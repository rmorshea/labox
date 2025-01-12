from __future__ import annotations

import abc
from collections import Counter
from collections.abc import Iterator
from collections.abc import Mapping
from typing import TYPE_CHECKING
from typing import ClassVar
from typing import Self
from typing import TypeVar

from lakery.common.exceptions import NotRegistered

if TYPE_CHECKING:
    from collections.abc import Sequence


K = TypeVar("K")
V = TypeVar("V")


class Registry(Mapping[K, V], abc.ABC):
    """A registry of named items."""

    value_description: ClassVar[str]
    """A description for the type of value"""

    def __init__(self, values: Sequence[V] = (), /) -> None:
        if not values:
            msg = "At least one item must be registered."
            raise ValueError(msg)

        items = [(self.get_key(i), i) for i in values]

        if conflicts := {n for n, c in Counter(k for k, _ in items).items() if c > 1}:
            msg = f"Conflicting {self.value_description.lower()} keys: {conflicts}"
            raise ValueError(msg)

        self._entries = dict(items)

    @abc.abstractmethod
    def get_key(self, value: V, /) -> K:
        """Get the key for the given value."""
        raise NotImplementedError

    def merge(self, *other: Registry[K, V]) -> Self:
        """Return a new registry that merges this one with the given ones."""
        new_values = [v for r in (self, *other) for v in r.values()]
        return self.__class__(new_values)

    def add(self, value: V) -> V:
        """Register the given value."""
        if (key := self.get_key(value)) not in self._entries:
            self._entries[key] = value
        elif (existing := self._entries[key]) is not value:
            msg = f"{self.value_description} {key!r} is registered as {existing}, not {value!r}."
            raise ValueError(msg)
        return value

    def check_registered(self, value: V) -> None:
        """Ensure that the given value is registered - raises a ValueError if not."""
        if not self.is_registered(value):
            msg = f"{self.value_description} {value!r} is not registered."
            raise NotRegistered(msg)

    def is_registered(self, value: V) -> bool:
        """Return whether the given value is registered."""
        if (key := self.get_key(value)) not in self._entries:
            return False
        return self._entries[key] is value

    def __getitem__(self, key: K) -> V:
        try:
            return self._entries[key]
        except KeyError:
            msg = f"{self.value_description} {key!r} is not registered."
            raise NotRegistered(msg) from None

    def __iter__(self) -> Iterator[K]:
        return iter(self._entries)

    def __len__(self) -> int:
        return len(self._entries)
