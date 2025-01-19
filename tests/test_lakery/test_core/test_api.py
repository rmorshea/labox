from lakery.core.model import Scalar
from lakery.core.model import Stream
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

SAMPLE_DATA = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]


def make_stream_model():
    async def stream():
        for item in SAMPLE_DATA:
            yield item

    return Stream(stream())


async def test_simple_value_data_saver_and_loader_usage():
    await assert_save_load_equivalence(Scalar(SAMPLE_DATA), basic_registries)


async def test_simple_stream_data_saver_and_loader_usage():
    await assert_save_load_equivalence(make_stream_model(), basic_registries)
