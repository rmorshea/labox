from lakery.core.model import StreamModel
from lakery.core.model import ValueModel
from tests.core_api_utils import assert_save_load_equivalence
from tests.core_context_utils import basic_registries

SAMPLE_DATA = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]


def make_value_model():
    return ValueModel(SAMPLE_DATA)


def make_stream_model():
    async def stream():
        for item in SAMPLE_DATA:
            yield item

    return StreamModel(stream())


async def test_simple_value_data_saver_and_loader_usage():
    await assert_save_load_equivalence(ValueModel(SAMPLE_DATA), basic_registries)


async def test_simple_stream_data_saver_and_loader_usage():
    await assert_save_load_equivalence(make_stream_model(), basic_registries)
