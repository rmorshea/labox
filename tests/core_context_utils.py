import shutil
from pathlib import Path

from lakery.core.context import Registries
from lakery.core.model import ModelRegistry
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.json import JsonStreamSerializer
from lakery.extra.lakery import LocalFileStorage

HERE = Path(__file__).parent
TEST_STORAGE_DIR = HERE / ".storage"

if TEST_STORAGE_DIR.exists():
    shutil.rmtree(TEST_STORAGE_DIR)

basic_registries = Registries(
    models=ModelRegistry.with_core_models(),
    storages=StorageRegistry([LocalFileStorage(TEST_STORAGE_DIR, mkdir=True)]),
    serializers=SerializerRegistry([JsonSerializer(), JsonStreamSerializer()]),
)
