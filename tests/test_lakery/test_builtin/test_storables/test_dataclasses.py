from dataclasses import dataclass
from dataclasses import field
from typing import Any

from labox._internal._utils import full_class_name
from labox.builtin.serializers.json import JsonSerializer
from labox.builtin.storables.dataclasses import StorableDataclass
from labox.builtin.storages.file import FileStorage
from labox.core.registry import Registry
from labox.extra.msgpack import MsgPackSerializer
from tests.core_registry_utils import basic_registry
from tests.core_storable_utils import make_storable_unpack_repack_test


@dataclass
class MyClass(StorableDataclass, class_id="bed9897b"):
    """A simple dataclass that is storable."""

    no_spec: Any
    spec_with_serializer: Any = field(metadata={"serializer": MsgPackSerializer})
    spec_with_storage: Any = field(metadata={"storage": FileStorage})
    spec_with_serializer_and_storage: Any = field(
        metadata={"serializer": MsgPackSerializer, "storage": FileStorage}
    )


SAMPLE = {"hello": "world", "answer": 42}
REGISTRY = Registry(registries=[basic_registry], storables=[MyClass], default_storage=True)
JSON_SERIALIZER = REGISTRY.get_serializer(JsonSerializer.name)
MSG_PACK_SERIALIZER = REGISTRY.get_serializer(MsgPackSerializer.name)
LOCAL_STORAGE = REGISTRY.get_storage(FileStorage.name)

test_dataclass_unpack_repack = make_storable_unpack_repack_test(
    [
        (
            MyClass(
                no_spec=SAMPLE,
                spec_with_serializer=SAMPLE,
                spec_with_storage=SAMPLE,
                spec_with_serializer_and_storage=SAMPLE,
            ),
            {
                "body": {
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {
                        "__labox__": "storable_dataclass",
                        "class_id": "bed9897b000000000000000000000000",
                        "class_name": full_class_name(MyClass),
                        "fields": {
                            "no_spec": {
                                "answer": 42,
                                "hello": "world",
                            },
                            "spec_with_serializer": {
                                "__labox__": "content",
                                "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                                "content_encoding": None,
                                "content_type": "application/msgpack",
                                "serializer_name": "labox.msgpack.value@v1",
                            },
                            "spec_with_serializer_and_storage": {
                                "__labox__": "ref",
                                "ref": "/ref/spec_with_serializer_and_storage",
                            },
                            "spec_with_storage": {
                                "__labox__": "ref",
                                "ref": "/ref/spec_with_storage",
                            },
                        },
                    },
                },
                "/ref/spec_with_storage": {
                    "serializer": None,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
                "/ref/spec_with_serializer_and_storage": {
                    "serializer": MSG_PACK_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
            },
        )
    ],
    REGISTRY,
)
