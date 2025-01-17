from typing import Any

from lakery.core.context import Registries
from lakery.core.serializer import SerializerRegistry
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.pydantic import StorageModel
from lakery.extra.pydantic import StorageSpec
from lakery.extra.tempfile import TemporaryDirectoryStorage


def test_dump_load_simple_model(basic_registries: Registries):
    msgpack_serializer = MsgPackSerializer()
    registries = Registries.merge(
        basic_registries,
        Registries(serializers=SerializerRegistry([msgpack_serializer])),
    )
    tempdir_storage = registries.storages[TemporaryDirectoryStorage.name]

    class SimpleModel(StorageModel, storage_id="1e76a0043a7d40a38daf87de09de1643"):
        auto: Any
        auto_serializer_auto_storage: Any
        manual_serializer_auto_storage: StorageSpec[Any, msgpack_serializer]
        manual_storage_auto_serializer: StorageSpec[Any, tempdir_storage]
        manual_serializer_manual_storage: StorageSpec[Any, msgpack_serializer, tempdir_storage]

    sample = {"hello": "world", "answer": 42}
    model = SimpleModel(
        auto=sample,
        auto_serializer_auto_storage=sample,
        manual_serializer_auto_storage=sample,
        manual_storage_auto_serializer=sample,
        manual_serializer_manual_storage=sample,
    )

    dump = model.storage_model_dump(registries)

    assert dump == {
        "data": {
            "value": {
                "auto": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                    "serializer_version": 1,
                },
                "auto_serializer_auto_storage": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                    "serializer_version": 1,
                },
                "manual_serializer_auto_storage": {
                    "__json_ext__": "content",
                    "content_base64": "gqVoZWxsb6V3b3JsZKZhbnN3ZXIq",
                    "content_encoding": None,
                    "content_type": "application/msgpack",
                    "serializer_name": "lakery.msgpack.value",
                    "serializer_version": 1,
                },
                "manual_storage_auto_serializer": {
                    "__json_ext__": "ref",
                    "ref": "ref.SimpleModel.manual_storage_auto_serializer.1",
                },
                "manual_serializer_manual_storage": {
                    "__json_ext__": "ref",
                    "ref": "ref.SimpleModel.manual_serializer_manual_storage.2",
                },
            },
            "serializer": msgpack_serializer,
            "storage": tempdir_storage,
        },
        "ref.SimpleModel.manual_serializer_manual_storage.2": {
            "serializer": msgpack_serializer,
            "storage": tempdir_storage,
            "value": {"answer": 42, "hello": "world"},
        },
        "ref.SimpleModel.manual_storage_auto_serializer.1": {
            "serializer": msgpack_serializer,
            "storage": tempdir_storage,
            "value": {"answer": 42, "hello": "world"},
        },
    }

    assert SimpleModel.storage_model_load(dump, registries) == model
