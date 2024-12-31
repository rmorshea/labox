from __future__ import annotations

import abc
from collections.abc import AsyncIterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Generic
from typing import LiteralString
from typing import Required
from typing import TypedDict
from typing import TypeVar
from typing import cast

from lakery.core._registry import Registry
from lakery.core.serializer import SerializerRegistry
from lakery.core.serializer import StreamDump
from lakery.core.serializer import StreamSerializer
from lakery.core.serializer import ValueDump
from lakery.core.serializer import ValueSerializer

ComponentMap = Mapping[str, ValueDump | StreamDump]
"""A mapping of components."""

T = TypeVar("T")
C = TypeVar("C", bound=ComponentMap, default=ComponentMap)


class Compositor(abc.ABC, Generic[T, C]):
    """A protocol for transforming data structures to and from serialized components."""

    name: LiteralString
    """The name of the composer."""
    types: tuple[type[T], ...]
    """The types that the composer can handle."""
    version: int

    @abc.abstractmethod
    def decompose(self, entity: T, serializers: SerializerRegistry) -> C:
        """Turn the given value into its serialized components."""
        raise NotImplementedError

    @abc.abstractmethod
    def recompose(self, components: C, serializers: SerializerRegistry) -> T:
        """Turn the given serialized components back into a value."""
        raise NotImplementedError


class BasicValue(Generic[T], TypedDict, total=False):
    """A value to be saved."""

    value: Required[T]
    serializer: ValueSerializer[T]


class BasicStream(Generic[T], TypedDict, total=False):
    """A stream of values to be saved."""

    stream: Required[AsyncIterable[T]]
    serializer: StreamSerializer[T]
    type: type[T]


class BasicCompositor(Compositor[BasicValue | BasicStream]):
    """A compositor for basic values and streams."""

    name = "lakery.default"
    types = ()
    version = 1
    _key = ""

    def decompose(
        self,
        entity: BasicValue | BasicStream,
        serializers: SerializerRegistry,
    ) -> ComponentMap:
        """Turn the given value or stream into its serialized components."""
        match entity:
            case {"value": value, "serializer": serializer}:
                return {self._key: serializer.dump_value(value)}
            case {"value": value}:
                serializer = serializers.infer_from_value_type(type(value))
                return {self._key: serializer.dump_value(value)}
            case {"stream": stream, "serializer": serializer}:
                return {self._key: serializer.dump_stream(stream)}
            case {"stream": stream, "type": type_}:
                serializer = serializers.infer_from_stream_type(type_)
                return {self._key: serializer.dump_stream(stream)}
            case {"stream": _}:
                msg = f"Stream type or serializer not provided: {entity}"
                raise ValueError(msg)
            case _:
                msg = f"Invalid entity: {entity}"
                raise ValueError(msg)

    def recompose(
        self,
        components: ComponentMap,
        serializers: SerializerRegistry,
    ) -> BasicValue | BasicStream:
        """Turn the given serialized components back into a value or stream."""
        match cmpt := components[self._key]:
            case {"content_value": _, "serializer_name": name}:
                serializer = cast("ValueSerializer", serializers.by_name[name])
                return {"value": serializer.load_value(cmpt)}
            case {"content_stream": _, "serializer_name": name}:
                serializer = cast("StreamSerializer", serializers.by_name[name])
                return {"stream": serializer.load_stream(cmpt)}
            case _:
                msg = f"Invalid components: {components}"
                raise ValueError(msg)


DEFAULT_COMPOSITOR = BasicCompositor()
"""The default compositor."""


class CompositorRegistry(Registry[Compositor]):
    """A registry for compositors."""

    item_description = "Compositor"

    def __init__(self, items: Sequence[Compositor]) -> None:
        super().__init__((DEFAULT_COMPOSITOR, *items))
        self.by_type = {type_: c for c in self.items for type_ in c.types}

    def infer_from_type(self, cls: type[T], /) -> Compositor[T]:
        """Get the first compositor that can handle the given type or its base classes."""
        for base in cls.mro():
            if item := self.by_type.get(base):
                return item
        msg = f"No {self.item_description.lower()} found for {cls}."
        raise ValueError(msg)
