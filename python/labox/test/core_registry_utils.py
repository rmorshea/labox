import shutil
from pathlib import Path

from labox.builtin.storages.file import FileStorage
from labox.core.registry import Registry

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)


basic_registry = Registry(
    # order matters for infering serializes/unpackers
    modules=[
        "labox.extra.pydantic",
        "labox.extra.msgpack",
        "labox.builtin.storables",
        "labox.builtin.serializers",
    ],
    storages=[FileStorage(TEST_STORAGE_DIR, mkdir=True)],
    default_storage=True,
)
