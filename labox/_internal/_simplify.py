from __future__ import annotations

from base64 import b64decode
from base64 import b64encode
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import TypedDict

if TYPE_CHECKING:
    from labox.core.registry import Registry
    from labox.core.serializer import Serializer


def dump_content_dict(value: Any, serializer: Serializer) -> LaboxContentDict:
    serialized_data = serializer.serialize_data(value)
    return LaboxContentDict(
        __labox__="content",
        content_base64=b64encode(serialized_data["data"]).decode("utf-8"),
        content_encoding=serialized_data["content_encoding"],
        content_type=serialized_data["content_type"],
        serializer_name=serializer.name,
    )


def load_content_dict(data: LaboxContentDict, registry: Registry) -> Any:
    serializer = registry.get_serializer(data["serializer_name"])
    if serializer is None:
        msg = f"Serializer {data['serializer_name']} not found in registry."
        raise ValueError(msg)
    return serializer.deserialize_data(
        {
            "data": b64decode(data["content_base64"].encode("utf-8")),
            "content_encoding": data.get("content_encoding"),
            "content_type": data["content_type"],
        }
    )


class LaboxContentDict(TypedDict):
    __labox__: Literal["content"]
    content_base64: str
    content_encoding: str | None
    content_type: str
    serializer_name: str


class LaboxRefDict(TypedDict):
    __labox__: Literal["ref"]
    ref: str
