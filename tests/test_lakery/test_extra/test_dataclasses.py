from dataclasses import dataclass
from dataclasses import field
from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.core.registries import ModelRegistry
from lakery.core.registries import RegistryCollection
from lakery.extra.dataclasses import StorageClass
from lakery.extra.json import JsonSerializer
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.os import FileStorage
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

local_storage = basic_registries.storages[FileStorage.name]
msgpack_serializer = basic_registries.serializers[MsgPackSerializer.name]
json_serializer = basic_registries.serializers[JsonSerializer.name]


@dataclass
class SampleModel(StorageClass, storage_model_config={"id": "1d3cd969", "version": 1}):
    field_with_no_meta: Any
    field_with_serializer: Any = field(
        metadata={"serializer": msgpack_serializer},
    )
    field_with_storage: Any = field(
        metadata={"storage": local_storage},
    )
    field_with_serializer_and_storage: Any = field(
        metadata={"serializer": json_serializer, "storage": local_storage}
    )


registries = RegistryCollection.merge(
    basic_registries,
    models=ModelRegistry([SampleModel]),
)


def test_dump_load_storage_model():
    sample = {"hello": "world", "answer": 42}

    model = SampleModel(
        field_with_no_meta=sample,
        field_with_serializer=sample,
        field_with_storage=sample,
        field_with_serializer_and_storage=sample,
    )

    contents = model.storage_model_dump(registries)

    assert contents == {
        "data": {
            "value": {
                "field_with_no_meta": {
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

    loaded_model = SampleModel.storage_model_load(contents, 1, registries)
    assert loaded_model == model


async def test_save_load_storage_model(session: AsyncSession):
    sample = {"hello": "world", "answer": 42}
    await assert_save_load_equivalence(
        SampleModel(
            field_with_no_meta=sample,
            field_with_serializer=sample,
            field_with_storage=sample,
            field_with_serializer_and_storage=sample,
        ),
        registries,
        session,
    )
