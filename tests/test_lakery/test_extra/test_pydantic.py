from typing import Annotated
from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.builtin.serializers.json import JsonSerializer
from lakery.builtin.storages import FileStorage
from lakery.core.registry import Registry
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.pydantic import StorageModel
from lakery.extra.pydantic import StorageSpec
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_registry_utils import basic_registry


class PydanticStorageModel(StorageModel, class_id="1e76a004"):
    no_spec: Any
    spec_with_serializer: Annotated[Any, StorageSpec(serializer=MsgPackSerializer)]
    spec_with_storage: Annotated[Any, StorageSpec(storage=FileStorage)]
    spec_with_serializer_and_storage: Annotated[
        Any, StorageSpec(serializer=MsgPackSerializer, storage=FileStorage)
    ]


registry = Registry(
    registries=[basic_registry],
    storables=[PydanticStorageModel],
    default_storage=True,
)
assert registry.get_default_storage()


def test_dump_load_storage_model():
    sample = {"hello": "world", "answer": 42}

    model = PydanticStorageModel(
        no_spec=sample,
        spec_with_serializer=sample,
        spec_with_storage=sample,
        spec_with_serializer_and_storage=sample,
    )

    unpacker = registry.infer_unpacker(PydanticStorageModel)
    contents = unpacker.unpack_object(model, registry)

    msgpack_serializer = registry.get_serializer(MsgPackSerializer.name)
    json_serializer = registry.get_serializer(JsonSerializer.name)
    local_storage = registry.get_storage(FileStorage.name)

    assert contents == {
        "data": {
            "value": {
                "no_spec": {
                    "__json_ext__": "content",
                    "content_base64": "eyJoZWxsbyI6IndvcmxkIiwiYW5zd2VyIjo0Mn0=",
                    "content_encoding": None,
                    "content_type": "application/json",
                    "serializer_name": JsonSerializer.name,
                },
                "spec_with_serializer": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": MsgPackSerializer.name,
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
            "serializer": json_serializer,
            "storage": local_storage,
        },
        "ref.PydanticStorageModel.spec_with_storage.1": {
            "serializer": json_serializer,
            "storage": local_storage,
            "value": {"answer": 42, "hello": "world"},
        },
        "ref.PydanticStorageModel.spec_with_serializer_and_storage.2": {
            "serializer": msgpack_serializer,
            "storage": local_storage,
            "value": {"answer": 42, "hello": "world"},
        },
    }

    loaded_model = unpacker.repack_object(PydanticStorageModel, contents, registry)
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
        registry,
        session,
    )
