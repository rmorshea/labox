from inspect import isawaitable
from typing import Any
from typing import TypeVar

from anysync.core import Awaitable
from anysync.core import Callable
from sqlalchemy import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import raiseload

from lakery.common.anyio import FutureResult
from lakery.core.api.loader import data_loader
from lakery.core.api.saver import data_saver
from lakery.core.model import BaseStorageModel
from lakery.core.registries import RegistryCollection
from lakery.core.schema import ManifestRecord

M = TypeVar("M", bound=BaseStorageModel)


def _default_compare(x: Any, y: Any) -> None:
    assert x == y


async def assert_save_load_equivalence(
    model: M,
    registries: RegistryCollection,
    session: AsyncSession,
    assertion: Callable[[M, M], None] = _default_compare,
) -> None:
    async with data_saver(session=session, registries=registries) as ms:
        future_record = ms.save_soon(model)

    record = await _get_realistic_manifest_record(session, future_record)

    async with data_loader(session=session, registries=registries) as ml:
        future_model = ml.load_soon(record, type(model))
    loaded_model = future_model.result()

    assertion(loaded_model, model)


async def assert_save_load_stream_equivalence(
    make_model: Callable[[], M],
    registries: RegistryCollection,
    session: AsyncSession,
    assertion: Callable[[M, M], Awaitable[None] | None] = _default_compare,
) -> None:
    original_model = make_model()
    async with data_saver(session=session, registries=registries) as ms:
        future_record = ms.save_soon(original_model)

    record = await _get_realistic_manifest_record(session, future_record)

    async with data_loader(session=session, registries=registries) as ml:
        future_model = ml.load_soon(record, type(original_model))
    loaded_model = future_model.result()

    asserted = assertion(loaded_model, make_model())
    if isawaitable(asserted):
        await asserted


async def _get_realistic_manifest_record(
    session: AsyncSession,
    future: FutureResult[ManifestRecord],
) -> ManifestRecord:
    returned_record = future.result()
    record_id = returned_record.id
    session.expunge(returned_record)  # expunge to clear any cached state

    # requery the record to create a more relistic test
    record = await session.scalar(
        select(ManifestRecord)
        .where(ManifestRecord.id == record_id)
        .options(raiseload(ManifestRecord.contents))
    )
    assert record is not None

    return record
