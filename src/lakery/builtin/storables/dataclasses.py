from __future__ import annotations

import dataclasses
from dataclasses import KW_ONLY
from dataclasses import asdict
from dataclasses import is_dataclass
from dataclasses import replace
from typing import TYPE_CHECKING
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import TypeVar
from typing import get_origin
from typing import get_type_hints

from lakery._internal._utils import frozenclass
from lakery.common.jsonext import dump_json_ext
from lakery.common.jsonext import load_json_ext
from lakery.core.storable import Storable
from lakery.core.unpacker import AnyUnpackedValue
from lakery.core.unpacker import Unpacker

if TYPE_CHECKING:
    from collections.abc import Mapping

    from lakery.common.types import TagMap
    from lakery.core.registry import Registry
    from lakery.core.serializer import Serializer
    from lakery.core.serializer import StreamSerializer
    from lakery.core.storage import Storage

T = TypeVar("T", default=Any)
__all__ = ("StorableClass",)


class _DataclassUnpacker(Unpacker["StorableClass"]):
    name = "lakery.builtin.dataclass@v1"

    def unpack_object(
        self,
        obj: StorableClass,
        registry: Registry,
    ) -> Mapping[str, AnyUnpackedValue]:
        if not is_dataclass(obj):
            msg = f"Expected a storable dataclass, got {type(obj)}"
            raise TypeError(msg)

        body, external = dump_json_ext(obj, registry)

        return {
            "data": {
                "serializer": obj.storable_class_serializer(registry),
                "storage": obj.storable_class_storage(registry),
                "value": body,
            },
            **external,
        }

    def repack_object(
        self,
        cls: type[StorableClass],
        contents: Mapping[str, AnyUnpackedValue],
        registry: Registry,
    ) -> StorableClass:
        contents = dict(contents)  # Make a copy to avoid modifying the original
        data = contents.pop("data", None)
        match data:
            case {"value": data_value}:
                pass
            case _:
                msg = "Expected 'data' to contain a 'value' key."
                raise KeyError(msg)
        kwargs = load_json_ext(data_value, registry, external=contents)
        return cls(**kwargs)

    def storable_class_serializer(self, registry: Registry) -> Serializer:
        """Return the serializer for the body of this storable class."""
        return registry.get_serializer_by_content_type("application/json")

    def storable_class_storage(self, registry: Registry) -> Storage:
        """Return the storage for the body of this storable class."""
        return registry.get_default_storage()


if TYPE_CHECKING:
    _FakeBase = dataclasses.DataclassInstance  # type: ignore
else:
    _FakeBase = object


class StorableClass(Storable, _FakeBase, unpacker=_DataclassUnpacker()):
    """A base for user-defined storable dataclasses."""

    _storable_class_annotation_metadata: ClassVar[Mapping[str, StorableSpec]] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        # Ensure this is always kw_only
        cls.__annotations__ = {"_": KW_ONLY, **cls.__annotations__}

        cls._storable_class_annotation_metadata = {**cls._storable_class_annotation_metadata}
        for name, anno in get_type_hints(cls, include_extras=True).items():
            if get_origin(anno) is Annotated:
                old_spec = spec = cls._storable_class_annotation_metadata.get(name, _EMPTY_SPEC)
                for item in anno.__args__:
                    if isinstance(item, StorableSpec):
                        spec = replace(spec, **asdict(item))
                        break
                if spec is old_spec:  # No changes to the spec
                    continue
                cls._storable_class_annotation_metadata[name] = spec


@frozenclass
class StorableSpec:
    """Metadata for a storable class field."""

    serializer: type[Serializer | StreamSerializer] | str | None = None
    """The serializer to use for this value."""
    storage: type[Storage] | str | None = None
    """The storage to use for this value."""
    tags: TagMap | None = None
    """Tags to apply to the stored value."""


_EMPTY_SPEC = StorableSpec()
