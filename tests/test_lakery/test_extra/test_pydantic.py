from typing import Annotated
from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from labox.builtin.serializers.json import JsonSerializer
from labox.builtin.storages import FileStorage
from labox.core.registry import Registry
from labox.extra.msgpack import MsgPackSerializer
from labox.extra.pydantic import ContentSpec
from labox.extra.pydantic import StorableModel
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_registry_utils import basic_registry
from tests.core_storable_utils import make_storable_unpack_repack_test


class MyModel(StorableModel, class_id="1e76a004"):
    no_spec: Any
    spec_with_serializer: Annotated[Any, ContentSpec(serializer=MsgPackSerializer)]
    spec_with_storage: Annotated[Any, ContentSpec(storage=FileStorage)]
    spec_with_serializer_and_storage: Annotated[
        Any, ContentSpec(serializer=MsgPackSerializer, storage=FileStorage)
    ]


SAMPLE = {"hello": "world", "answer": 42}

REGISTRY = Registry(registries=[basic_registry], storables=[MyModel], default_storage=True)
JSON_SERIALIZER = REGISTRY.get_serializer(JsonSerializer.name)
MSG_PACK_SERIALIZER = REGISTRY.get_serializer(MsgPackSerializer.name)
LOCAL_STORAGE = REGISTRY.get_storage(FileStorage.name)


test_unpack_repack_storable_model = make_storable_unpack_repack_test(
    [
        (
            MyModel(
                no_spec=SAMPLE,
                spec_with_serializer=SAMPLE,
                spec_with_storage=SAMPLE,
                spec_with_serializer_and_storage=SAMPLE,
            ),
            {
                "body": {
                    "value": {
                        "no_spec": {
                            "__labox__": "content",
                            "content_base64": "eyJoZWxsbyI6IndvcmxkIiwiYW5zd2VyIjo0Mn0=",
                            "content_encoding": None,
                            "content_type": "application/json",
                            "serializer_name": JsonSerializer.name,
                        },
                        "spec_with_serializer": {
                            "__labox__": "content",
                            "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                            "content_encoding": None,
                            "content_type": "application/msgpack",
                            "serializer_name": MsgPackSerializer.name,
                        },
                        "spec_with_storage": {
                            "__labox__": "ref",
                            "ref": "ref.MyModel.spec_with_storage.1",
                        },
                        "spec_with_serializer_and_storage": {
                            "__labox__": "ref",
                            "ref": "ref.MyModel.spec_with_serializer_and_storage.2",
                        },
                    },
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                },
                "ref.MyModel.spec_with_storage.1": {
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
                "ref.MyModel.spec_with_serializer_and_storage.2": {
                    "serializer": MSG_PACK_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
            },
        )
    ],
    REGISTRY,
)


async def test_save_load_storage_model(session: AsyncSession):
    await assert_save_load_equivalence(
        MyModel(
            no_spec=SAMPLE,
            spec_with_serializer=SAMPLE,
            spec_with_storage=SAMPLE,
            spec_with_serializer_and_storage=SAMPLE,
        ),
        REGISTRY,
        session,
    )
