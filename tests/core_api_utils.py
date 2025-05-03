from inspect import isawaitable
from typing import Any
from typing import TypeVar

from anysync.core import Awaitable
from anysync.core import Callable
from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.core.api.loader import data_loader
from lakery.core.api.saver import data_saver
from lakery.core.context import Registries
from lakery.core.model import BaseStorageModel

M = TypeVar("M", bound=BaseStorageModel)


def _default_compare(x: Any, y: Any) -> None:
    assert x == y


async def assert_save_load_equivalence(
    model: M,
    registries: Registries,
    session: AsyncSession,
    assertion: Callable[[M, M], Awaitable[None] | None] = _default_compare,
) -> None:
    async with data_saver(session=session, registries=registries) as ms:
        future_record = ms.save_soon(model)
    record = future_record.result()

    async with data_loader(session=session, registries=registries) as ml:
        future_model = ml.load_soon(record, type(model))
    loaded_model = future_model.result()

    asserted = assertion(loaded_model, model)
    if isawaitable(asserted):
        await asserted
