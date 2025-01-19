from dataclasses import dataclass
from dataclasses import field
from typing import Any

from lakery.core.context import Registries
from lakery.core.serializer import SerializerRegistry
from lakery.extra.dataclasses import StorageModel
from lakery.extra.dataclasses import get_model_registry
from lakery.extra.json import JsonSerializer
from lakery.extra.lakery import LocalFileStorage
from lakery.extra.msgpack import MsgPackSerializer
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

registries = Registries.merge(
    basic_registries,
    Registries(serializers=SerializerRegistry([MsgPackSerializer()])),
)

local_storage = registries.storages[LocalFileStorage.name]
msgpack_serializer = registries.serializers[MsgPackSerializer.name]
json_serializer = registries.serializers[JsonSerializer.name]


@dataclass
class DataclasstorageModel(StorageModel, storage_id="d1d3cd96964a45bbb718de26f2671b87"):
    default: Any
    field_with_serializer: Any = field(
        metadata={"serializer": msgpack_serializer},
    )
    field_with_storage: Any = field(
        metadata={"storage": local_storage},
    )
    field_with_serializer_and_storage: Any = field(
        metadata={"serializer": json_serializer, "storage": local_storage}
    )


registries = Registries.merge(registries, Registries(models=get_model_registry()))


def test_dump_load_storage_model():
    sample = {"hello": "world", "answer": 42}

    model = DataclasstorageModel(
        default=sample,
        field_with_serializer=sample,
        field_with_storage=sample,
        field_with_serializer_and_storage=sample,
    )

    dump = model.storage_model_dump(registries)

    assert dump == {
        "data": {
            "value": {
                "default": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                },
                "field_with_serializer": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                },
                "field_with_storage": {
                    "__json_ext__": "ref",
                    "ref": "/field_with_storage",
                },
                "field_with_serializer_and_storage": {
                    "__json_ext__": "ref",
                    "ref": "/field_with_serializer_and_storage",
                },
            },
            "serializer": msgpack_serializer,
            "storage": local_storage,
        },
        "/field_with_storage": {
            "value": {
                "answer": 42,
                "hello": "world",
            },
            "serializer": None,
            "storage": local_storage,
        },
        "/field_with_serializer_and_storage": {
            "value": {
                "answer": 42,
                "hello": "world",
            },
            "serializer": json_serializer,
            "storage": local_storage,
        },
    }

    loaded_model = DataclasstorageModel.storage_model_load(dump, registries)
    assert loaded_model == model


async def test_save_load_storage_model():
    sample = {"hello": "world", "answer": 42}
    await assert_save_load_equivalence(
        DataclasstorageModel(
            default=sample,
            field_with_serializer=sample,
            field_with_storage=sample,
            field_with_serializer_and_storage=sample,
        ),
        registries,
    )
