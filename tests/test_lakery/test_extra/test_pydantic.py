from typing import Annotated
from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.core.registry import ModelRegistry
from lakery.core.registry import RegistryCollection
from lakery.extra.json import JsonSerializer
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.os import FileStorage
from lakery.extra.pydantic import StorageModel
from lakery.extra.pydantic import StorageSpec
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

local_storage = basic_registries.storages[FileStorage.name]
msgpack_serializer = basic_registries.serializers[MsgPackSerializer.name]
json_serializer = basic_registries.serializers[JsonSerializer.name]


class PydanticStorageModel(StorageModel, storage_model_config={"id": "1e76a004", "version": 1}):
    no_spec: Any
    spec_with_serializer: Annotated[Any, StorageSpec(serializer=msgpack_serializer)]
    spec_with_storage: Annotated[Any, StorageSpec(storage=local_storage)]
    spec_with_serializer_and_storage: Annotated[
        Any, StorageSpec(serializer=json_serializer, storage=local_storage)
    ]


registries = RegistryCollection.merge(
    basic_registries,
    models=ModelRegistry([PydanticStorageModel]),
)


def test_dump_load_storage_model():
    sample = {"hello": "world", "answer": 42}

    model = PydanticStorageModel(
        no_spec=sample,
        spec_with_serializer=sample,
        spec_with_storage=sample,
        spec_with_serializer_and_storage=sample,
    )

    contents = model.storage_model_dump(registries)

    assert contents == {
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

    loaded_model = PydanticStorageModel.storage_model_load(contents, 1, registries)
    assert loaded_model == model


async def test_save_load_storage_model(session: AsyncSession):
    sample = {"hello": "world", "answer": 42}
    await assert_save_load_equivalence(
        PydanticStorageModel(
            no_spec=sample,
            spec_with_serializer=sample,
            spec_with_storage=sample,
            spec_with_serializer_and_storage=sample,
        ),
        registries,
        session,
    )
