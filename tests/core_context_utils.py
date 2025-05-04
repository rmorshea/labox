import shutil
from pathlib import Path

from lakery.core.model import ModelRegistry
from lakery.core.registries import RegistryCollection
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.json import JsonStreamSerializer
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.msgpack import MsgPackStreamSerializer
from lakery.extra.os import FileStorage

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)

basic_registries = RegistryCollection(
    models=ModelRegistry.from_modules(
        "lakery.common.models",
        "lakery.extra.pydantic",
        "lakery.extra.dataclasses",
    ),
    storages=StorageRegistry(
        default=FileStorage(TEST_STORAGE_DIR, mkdir=True),
    ),
    serializers=SerializerRegistry(
        [
            JsonSerializer(),
            JsonStreamSerializer(),
            MsgPackSerializer(),
            MsgPackStreamSerializer(),
        ]
    ),
)
