from collections.abc import AsyncIterable
from typing import Annotated
from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from labox.builtin.serializers.json import JsonSerializer
from labox.builtin.storages import FileStorage
from labox.core.registry import Registry
from labox.extra.msgpack import MsgPackSerializer
from labox.extra.pydantic import StorableSpec
from labox.extra.pydantic import StorableModel
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_registry_utils import basic_registry
from tests.core_storable_utils import make_storable_unpack_repack_test


class BasicModel(StorableModel, class_id="1e76a004"):
    no_spec: Any
    spec_with_serializer: Annotated[Any, StorableSpec(serializer=MsgPackSerializer)]
    spec_with_storage: Annotated[Any, StorableSpec(storage=FileStorage)]
    spec_with_serializer_and_storage: Annotated[
        Any, StorableSpec(serializer=MsgPackSerializer, storage=FileStorage)
    ]


class ModelWithAsyncIterable(StorableModel, class_id="2f76b005"):
    any_async_iterable: AsyncIterable[Any]
    async_iterable_with_type: AsyncIterable[dict]


REGISTRY = Registry(registries=[basic_registry], storables=[BasicModel], default_storage=True)
JSON_SERIALIZER = REGISTRY.get_serializer(JsonSerializer.name)
MSG_PACK_SERIALIZER = REGISTRY.get_serializer(MsgPackSerializer.name)
LOCAL_STORAGE = REGISTRY.get_storage(FileStorage.name)


async def make_sample_stream() -> AsyncIterable[Any]:
    yield {"key": "value"}
    yield {"another_key": "another_value"}


SAMPLE_STREAM = make_sample_stream()
SAMPLE = {"hello": "world", "answer": 42}


test_unpack_repack_storable_model = make_storable_unpack_repack_test(
    [
        (
            "model-with-and-without-specs",
            BasicModel(
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
                            "ref": "ref.BasicModel.spec_with_storage.1",
                        },
                        "spec_with_serializer_and_storage": {
                            "__labox__": "ref",
                            "ref": "ref.BasicModel.spec_with_serializer_and_storage.2",
                        },
                    },
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                },
                "ref.BasicModel.spec_with_storage.1": {
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
                "ref.BasicModel.spec_with_serializer_and_storage.2": {
                    "serializer": MSG_PACK_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {"answer": 42, "hello": "world"},
                },
            },
        ),
        (
            "model-with-async-iterable",
            ModelWithAsyncIterable(
                any_async_iterable=SAMPLE_STREAM,
                async_iterable_with_type=SAMPLE_STREAM,
            ),
            {
                "body": {
                    "serializer": JSON_SERIALIZER,
                    "storage": LOCAL_STORAGE,
                    "value": {
                        "any_async_iterable": {
                            "__labox__": "ref",
                            "ref": "ref.ModelWithAsyncIterable.any_async_iterable.1",
                        },
                        "async_iterable_with_type": {
                            "__labox__": "ref",
                            "ref": "ref.ModelWithAsyncIterable.async_iterable_with_type.2",
                        },
                    },
                },
                "ref.ModelWithAsyncIterable.any_async_iterable.1": {
                    "serializer": None,
                    "storage": LOCAL_STORAGE,
                    "value_stream": SAMPLE_STREAM,
                },
                "ref.ModelWithAsyncIterable.async_iterable_with_type.2": {
                    "serializer": None,
                    "storage": LOCAL_STORAGE,
                    "value_stream": SAMPLE_STREAM,
                },
            },
        ),
    ],
    REGISTRY,
)


async def test_save_load_storage_model(session: AsyncSession):
    await assert_save_load_equivalence(
        BasicModel(
            no_spec=SAMPLE,
            spec_with_serializer=SAMPLE,
            spec_with_storage=SAMPLE,
            spec_with_serializer_and_storage=SAMPLE,
        ),
        REGISTRY,
        session,
    )
