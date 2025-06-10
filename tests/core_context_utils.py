import shutil
from pathlib import Path

from lakery.core.registry import Registry
from lakery.extra.json import JsonSerializer
from lakery.extra.json import JsonStreamSerializer
from lakery.extra.msgpack import MsgPackSerializer
from lakery.extra.msgpack import MsgPackStreamSerializer
from lakery.extra.os import FileStorage

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)

basic_registry = Registry.from_modules(
    "lakery.builtin.storables",
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
