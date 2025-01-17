from typing import Any

from lakery.core.api.loader import data_loader
from lakery.core.api.saver import model_saver
from lakery.core.context import EMPTY_MODEL_REGISTRY
from lakery.core.context import EMPTY_SERIALIZER_REGISTRY
from lakery.core.context import EMPTY_STORAGE_REGISTRY
from lakery.core.context import registries
from lakery.core.model import BaseStorageModel
from lakery.core.model import ModelRegistry
from lakery.core.serializer import SerializerRegistry
from lakery.core.storage import StorageRegistry


async def assert_save_load_equivalence(
    model: BaseStorageModel[Any],
    models: ModelRegistry = EMPTY_MODEL_REGISTRY,
    serializers: SerializerRegistry = EMPTY_SERIALIZER_REGISTRY,
    storages: StorageRegistry = EMPTY_STORAGE_REGISTRY,
) -> None:
    with registries(models=models, serializers=serializers, storages=storages):
        async with model_saver() as ms:
            ms.save_soon("sample", model)

        async with data_loader() as ml:
            model_future = ml.load_soon(type(model), name="sample")
        loaded_model = model_future.result()

        assert loaded_model == model
