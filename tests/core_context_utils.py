import shutil
from pathlib import Path

from lakery.builtin.serializers.json import JsonSerializer
from lakery.builtin.serializers.json import JsonStreamSerializer
from lakery.builtin.storages.file import FileStorage
from lakery.core.registry import Registry
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.msgpack import MsgPackStreamSerializer

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)

basic_registry = Registry.from_modules(
    "lakery.builtin.storables",
    "lakery.builtin.serializers",
    "lakery.extra.pydantic",
).merge(
    storages=[FileStorage(TEST_STORAGE_DIR, mkdir=True)],
    serializers=[
        JsonSerializer(),
        JsonStreamSerializer(),
        MsgPackSerializer(),
        MsgPackStreamSerializer(),
    ],
)
