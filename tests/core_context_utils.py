import shutil
from pathlib import Path

from lakery.builtin import get_model_registry
from lakery.core.context import Registries
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry
from lakery.stdlib.json import JsonSerializer
from lakery.stdlib.json import JsonStreamSerializer
from lakery.stdlib.os import FileStorage

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():  # nocov
    shutil.rmtree(TEST_STORAGE_DIR)

basic_registries = Registries(
    models=get_model_registry(),
    storages=StorageRegistry(default=FileStorage(TEST_STORAGE_DIR, mkdir=True)),
    serializers=SerializerRegistry([JsonSerializer(), JsonStreamSerializer()]),
)
