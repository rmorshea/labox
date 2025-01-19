from typing import Any

from lakery.core.api.loader import data_loader
from lakery.core.api.saver import data_saver
from lakery.core.context import Registries
from lakery.core.model import BaseStorageModel


async def assert_save_load_equivalence(
    model: BaseStorageModel[Any], registries: Registries
) -> None:
    async with data_saver(registries=registries) as ms:
        future_record = ms.save_soon(model)
    record = future_record.result()

    async with data_loader(registries=registries) as ml:
        future_model = ml.load_soon(record, type(model))
    loaded_model = future_model.result()

    assert loaded_model == model
