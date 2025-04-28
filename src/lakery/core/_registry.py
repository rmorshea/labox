from __future__ import annotations

import abc
from collections import Counter
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from importlib import import_module
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Self
from typing import TypeIs
from typing import TypeVar

from lakery.common.exceptions import NotRegistered

if TYPE_CHECKING:
    from types import ModuleType

K = TypeVar("K")
V = TypeVar("V")


class Registry(Mapping[K, V], abc.ABC):
    """A registry of named items."""

    value_description: ClassVar[str]
    """A description for the type of value"""

    def __init__(self, values: Iterable[V] = (), /, *, ignore_conflicts: bool = False) -> None:
        items = [(self.get_key(i), i) for i in values]

        if not ignore_conflicts and (
            conflicts := {n for n, c in Counter(k for k, _ in items).items() if c > 1}
        ):
            msg = f"Conflicting {self.value_description.lower()} keys: {conflicts}"
            raise ValueError(msg)

        self._entries: Mapping[K, V] = dict(items)

    @abc.abstractmethod
    def get_key(self, value: V, /) -> K:
        """Get the key for the given value."""
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def can_register(cls, value: Any, /) -> TypeIs[V]:
        """Return whether the given value is a valid registry value."""
        raise NotImplementedError

    @classmethod
    def merge(cls, *registries: Any, ignore_conflicts: bool = False) -> Self:
        """Return a new registry that merges this one with the given ones."""
        new_values = [v for r in registries for v in r.values()]
        return cls(new_values, ignore_conflicts=ignore_conflicts)

    def check_registered(self, value: V) -> None:
        """Ensure that the given value is registered - raises a ValueError if not."""
        if not self.is_registered(value):
            key = self.get_key(value)
            msg = f"{self.value_description} {value!r} with key {key!r} is not registered."
            raise NotRegistered(msg)

    def is_registered(self, value: V) -> bool:
        """Return whether the given value is registered."""
        if (key := self.get_key(value)) not in self._entries:
            return False
        return self._entries[key] is value

    @classmethod
    def from_modules(
        cls,
        *modules: ModuleType | str,
        ignore_conflicts: bool = False,
        **kwargs: Any,
    ) -> Self:
        """Create a registry from a module."""
        model_types: list[V] = []
        for mod in modules:
            if isinstance(mod, str):
                mod = import_module(mod)
            if not hasattr(mod, "__all__"):
                msg = f"Module {mod} must have an '__all__' attribute."
                raise ValueError(msg)
            for name in mod.__all__:
                maybe = getattr(mod, name, None)
                if cls.can_register(maybe):
                    model_types.append(maybe)
        return cls(model_types, ignore_conflicts=ignore_conflicts, **kwargs)

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

    def __hash__(self) -> int:
        return hash((type(self), tuple(self._entries)))

    def __repr__(self) -> str:
        items = ", ".join(f"{k!r}: {v!r}" for k, v in self._entries.items())
        return f"{type(self).__name__}({items})"
