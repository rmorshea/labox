from __future__ import annotations

import dataclasses
from dataclasses import KW_ONLY
from dataclasses import is_dataclass
from typing import TYPE_CHECKING
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import TypedDict
from typing import TypeVar
from typing import Unpack

from lakery._internal._utils import frozenclass
from lakery.common.jsonext import dump_json_ext
from lakery.common.jsonext import load_json_ext
from lakery.core.storable import Storable
from lakery.core.storable import StorableConfigDict
from lakery.core.unpacker import AnyUnpackedValue
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage

T = TypeVar("T", default=Any)
__all__ = ("StorableDataclass",)


class _StorableDataclassUnpacker(Unpacker["StorableDataclass"]):
    name = "lakery.builtin.dataclass@v1"

    def unpack_object(
        self,
        obj: StorableDataclass,
        registry: Registry,
    ) -> Mapping[str, AnyUnpackedValue]:
        if not is_dataclass(obj):
            msg = f"Expected a storable dataclass, got {type(obj)}"
            raise TypeError(msg)

        body, external = dump_json_ext(
            {f.name: getattr(obj, f.name) for f in dataclasses.fields(obj)},
            registry,
        )

        return {
            "data": {
                "serializer": obj.storable_dataclass_serializer(registry),
                "storage": obj.storable_dataclass_storage(registry),
                "value": body,
            },
            **external,
        }

    def repack_object(
        self,
        cls: type[StorableDataclass],
        contents: Mapping[str, AnyUnpackedValue],
        registry: Registry,
    ) -> StorableDataclass:
        contents = dict(contents)  # Make a copy to avoid modifying the original
        data = contents.pop("data", None)
        match data:
            case {"value": data_value}:
                pass
            case _:
                msg = f"Expected a 'data' key with 'value', got {data}"
                raise KeyError(msg)
        kwargs = load_json_ext(data_value, registry, external=contents)
        return cls(**kwargs)


if TYPE_CHECKING:
    _FakeBase = dataclasses.DataclassInstance  # type: ignore
else:
    _FakeBase = object


class Config(StorableConfigDict, total=False):
    extra_fields: Literal["ignore", "forbid"]
    field_specs: Mapping[str, ContentSpec]


class StorableDataclass(Storable, _FakeBase, unpacker=_StorableDataclassUnpacker()):
    """A base for user-defined storable dataclasses."""

    _storable_class_info: ClassVar[_StorableDataclassInfo] = {
        "extra_fields": "forbid",
    }

    def __init_subclass__(cls, **kwargs: Unpack[Config]):
        # Ensure this is always kw_only
        cls.__annotations__ = {"_": KW_ONLY, **cls.__annotations__}
        cls._storable_class_info = {
            "extra_fields": kwargs.get(
                "extra_fields",
                cls._storable_class_info["extra_fields"],
            ),
        }

    def storable_dataclass_serializer(self, registry: Registry) -> Serializer:
        """Return the serializer for the body of this storable class."""
        return registry.get_serializer_by_content_type("application/json")

    def storable_dataclass_storage(self, registry: Registry) -> Storage:
        """Return the storage for the body of this storable class."""
        return registry.get_default_storage()


class _StorableDataclassInfo(TypedDict):
    """Metadata for a storable class."""

    extra_fields: Literal["ignore", "forbid"]


@frozenclass
class ContentSpec:
    """Metadata for the field of a storable class."""

    serializer: type[Serializer | StreamSerializer] | None = None
    """The serializer to use for this value."""
    storage: type[Storage] | None = None
    """The storage to use for this value."""


_EMPTY_SPEC = ContentSpec()
