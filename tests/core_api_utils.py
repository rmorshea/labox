from typing import Any

from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.core.api.loader import data_loader
from lakery.core.api.saver import data_saver
from lakery.core.context import Registries
from lakery.core.model import BaseStorageModel


async def assert_save_load_equivalence(
    model: BaseStorageModel[Any],
    registries: Registries,
    session: AsyncSession,
) -> None:
    async with data_saver(session=session, registries=registries) as ms:
        future_record = ms.save_soon(model)
    record = future_record.result()

    async with data_loader(session=session, registries=registries) as ml:
        future_model = ml.load_soon(record, type(model))
    loaded_model = future_model.result()

    assert loaded_model == model
