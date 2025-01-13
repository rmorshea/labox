from typing import Any

from lakery.core.api.loader import data_loader
from lakery.core.api.saver import model_saver
from lakery.core.model import StorageModel


async def assert_save_load_equivalence(model: StorageModel[Any]) -> None:
    async with model_saver() as ms:
        ms.save_soon("sample", model)

    async with data_loader() as ml:
        model_future = ml.load_soon(type(model), name="sample")
    loaded_model = model_future.result()

    assert loaded_model == model
