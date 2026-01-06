from sqlalchemy.ext.asyncio.session import AsyncSession

from labox.builtin.storables import StorableStream
from labox.builtin.storables import StorableValue
from labox.core import load_one
from labox.core import save_one
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_api_utils import assert_save_load_stream_equivalence
from tests.core_registry_utils import basic_registry

SAMPLE_DATA = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]


async def test_simple_value_data_saver_and_loader_usage(session: AsyncSession):
    await assert_save_load_equivalence(StorableValue(SAMPLE_DATA), basic_registry, session)


def make_stream_model():
    async def stream():
        for item in SAMPLE_DATA:
            yield item

    return StorableStream(stream())


async def assert_streamed_equal(s1: StorableStream, s2: StorableStream) -> None:
    assert [x async for x in s1.value_stream] == [y async for y in s2.value_stream]


async def test_simple_stream_data_saver_and_loader_usage(session: AsyncSession):
    await assert_save_load_stream_equivalence(
        make_stream_model,
        basic_registry,
        session,
        assert_streamed_equal,
    )


async def test_save_load_one(session: AsyncSession):
    model = StorableValue(SAMPLE_DATA)
    record = await save_one(model, registry=basic_registry, session=session)
    loaded_model = await load_one(record, type(model), registry=basic_registry, session=session)

    assert loaded_model == model
    assert isinstance(loaded_model, StorableValue)
    assert loaded_model.value == SAMPLE_DATA
