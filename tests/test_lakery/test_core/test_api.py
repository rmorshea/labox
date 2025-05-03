from sqlalchemy.ext.asyncio.session import AsyncSession

from lakery.common.models import Singular
from lakery.common.models import Streamed
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_api_utils import assert_save_load_stream_equivalence
from tests.core_context_utils import basic_registries

SAMPLE_DATA = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]


async def test_simple_value_data_saver_and_loader_usage(session: AsyncSession):
    await assert_save_load_equivalence(Singular(SAMPLE_DATA), basic_registries, session)


def make_stream_model():
    async def stream():
        for item in SAMPLE_DATA:
            yield item

    return Streamed(stream())


async def assert_streamed_equal(s1: Streamed, s2: Streamed) -> None:
    assert [x async for x in s1.stream] == [y async for y in s2.stream]


async def test_simple_stream_data_saver_and_loader_usage(session: AsyncSession):
    await assert_save_load_stream_equivalence(
        make_stream_model,
        basic_registries,
        session,
        assert_streamed_equal,
    )
