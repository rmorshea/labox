import abc
from typing import ClassVar
from typing import LiteralString

from labox._internal._utils import validate_versioned_class_name


class Component(abc.ABC):
    """Base for Labox components."""

    name: ClassVar[LiteralString]
    """The globally unique name of the component."""

    def __init_subclass__(cls) -> None:
        validate_versioned_class_name(cls)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, type(self)) and self.name == other.name

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"
