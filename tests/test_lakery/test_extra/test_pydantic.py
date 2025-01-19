from typing import Any

from lakery.core.context import Registries
from lakery.core.serializer import SerializerRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.os import FileStorage
from lakery.extra.pydantic import StorageModel
from lakery.extra.pydantic import StorageSpec
from lakery.extra.pydantic import get_model_registry
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

registries = Registries.merge(
    basic_registries,
    Registries(serializers=SerializerRegistry([MsgPackSerializer()])),
)

local_storage = registries.storages[FileStorage.name]
msgpack_serializer = registries.serializers[MsgPackSerializer.name]
json_serializer = registries.serializers[JsonSerializer.name]


class PydanticStorageModel(StorageModel, storage_id="1e76a0043a7d40a38daf87de09de1643"):
    no_spec: Any
    spec_with_serializer: StorageSpec[Any, msgpack_serializer]
    spec_with_storage: StorageSpec[Any, local_storage]
    spec_with_serializer_and_storage: StorageSpec[Any, json_serializer, local_storage]


registries = Registries.merge(registries, Registries(models=get_model_registry()))


def test_dump_load_storage_model():
    sample = {"hello": "world", "answer": 42}

    model = PydanticStorageModel(
        no_spec=sample,
        spec_with_serializer=sample,
        spec_with_storage=sample,
        spec_with_serializer_and_storage=sample,
    )

    manifests = model.storage_model_dump(registries)

    assert manifests == {
        "data": {
            "value": {
                "no_spec": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                },
                "spec_with_serializer": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                },
                "spec_with_storage": {
                    "__json_ext__": "ref",
                    "ref": "ref.PydanticStorageModel.spec_with_storage.1",
                },
                "spec_with_serializer_and_storage": {
                    "__json_ext__": "ref",
                    "ref": "ref.PydanticStorageModel.spec_with_serializer_and_storage.2",
                },
            },
            "serializer": msgpack_serializer,
            "storage": local_storage,
        },
        "ref.PydanticStorageModel.spec_with_storage.1": {
            "serializer": msgpack_serializer,
            "storage": local_storage,
            "value": {"answer": 42, "hello": "world"},
        },
        "ref.PydanticStorageModel.spec_with_serializer_and_storage.2": {
            "serializer": json_serializer,
            "storage": local_storage,
            "value": {"answer": 42, "hello": "world"},
        },
    }

    loaded_model = PydanticStorageModel.storage_model_load(manifests, registries)
    assert loaded_model == model


async def test_save_load_storage_model():
    sample = {"hello": "world", "answer": 42}
    await assert_save_load_equivalence(
        PydanticStorageModel(
            no_spec=sample,
            spec_with_serializer=sample,
            spec_with_storage=sample,
            spec_with_serializer_and_storage=sample,
        ),
        registries,
    )
