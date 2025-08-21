from contextlib import AsyncExitStack
from inspect import isawaitable
from typing import Any
from typing import TypeVar

from anysync.core import Awaitable
from anysync.core import Callable
from sqlalchemy import select
from sqlalchemy.ext.asyncio.session import AsyncSession
from sqlalchemy.orm import raiseload

from labox.core.api.loader import new_loader
from labox.core.api.saver import new_saver
from labox.core.database import ManifestRecord
from labox.core.registry import Registry
from labox.core.storable import Storable

S = TypeVar("S", bound=Storable)


def _default_compare(x: Any, y: Any) -> None:
    assert x == y


async def assert_save_load_equivalence(
    obj: S,
    registry: Registry,
    session: AsyncSession,
    assertion: Callable[[S, S], None] = _default_compare,
) -> None:
    async with new_saver(session=session, registry=registry) as ms:
        future_record = ms.save_soon(obj)

    record = await _get_realistic_manifest_record(session, future_record.value)

    async with new_loader(session=session, registry=registry) as ml:
        future_obj = ml.load_soon(record, type(obj))
    loaded_obj = future_obj.value

    assertion(loaded_obj, obj)


async def assert_save_load_stream_equivalence(
    make_storable: Callable[[], S],
    registry: Registry,
    session: AsyncSession,
    assertion: Callable[[S, S], Awaitable[None] | None] = _default_compare,
) -> None:
    original_obj = make_storable()
    async with new_saver(session=session, registry=registry) as ms:
        future_record = ms.save_soon(original_obj)

    record = await _get_realistic_manifest_record(session, future_record.value)

    async with AsyncExitStack() as stack:
        async with new_loader(session=session, registry=registry, stack=stack) as ml:
            future_obj = ml.load_soon(record, type(original_obj))
        loaded_obj = future_obj.value

        asserted = assertion(loaded_obj, make_storable())
        if isawaitable(asserted):
            await asserted


async def _get_realistic_manifest_record(
    session: AsyncSession,
    given: ManifestRecord,
) -> ManifestRecord:
    record_id = given.id
    session.expunge(given)  # expunge to clear any cached state

    # requery the record to create a more relistic test
    record = await session.scalar(
        select(ManifestRecord)
        .where(ManifestRecord.id == record_id)
        .options(raiseload(ManifestRecord.contents))
    )
    assert record is not None

    return record
