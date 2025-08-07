import abc
from dataclasses import KW_ONLY
from typing import LiteralString

from labox._internal._utils import frozenclass


@frozenclass(kw_only=False)
class Component(abc.ABC):
    """Base for Labox components."""

    name: LiteralString
    """The globally unique name of the component."""

    _: KW_ONLY
