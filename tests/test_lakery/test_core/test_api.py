from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.common.storables import StorableStream
from lakery.common.storables import StorableValue
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_api_utils import assert_save_load_stream_equivalence
from tests.core_context_utils import basic_registry

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
