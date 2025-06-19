import shutil
from pathlib import Path

from lakery.builtin.storages.file import FileStorage
from lakery.core.registry import Registry

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)


basic_registry = Registry(
    # order matters for infering serializes/unpackers
    "lakery.extra.pydantic",
    "lakery.extra.msgpack",
    "lakery.builtin.storables",
    "lakery.builtin.serializers",
    storages=[FileStorage(TEST_STORAGE_DIR, mkdir=True)],
    use_default_storage=True,
)
