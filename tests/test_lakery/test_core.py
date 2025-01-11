from lakery.core.api.loader import data_loader
from lakery.core.api.saver import model_saver
from lakery.core.model import StreamModel
from lakery.core.model import ValueModel

SAMPLE_DATA = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]


def make_value_model():
    return ValueModel(SAMPLE_DATA)


def make_stream_model():
    async def stream():
        for item in SAMPLE_DATA:
            yield item

    return StreamModel(stream())


async def test_simple_value_data_saver_and_loader_usage():
    original_model = make_value_model()

    async with model_saver() as ms:
        ms.save_soon("sample", original_model)

    async with data_loader() as ml:
        model_future = ml.load_soon(ValueModel, name="sample")
    loaded_model = model_future.result()

    assert loaded_model == original_model


async def test_simple_stream_data_saver_and_loader_usage():
    original_model = make_stream_model()

    async with model_saver() as ms:
        ms.save_soon("sample", original_model)

    async with data_loader() as ml:
        model_future = ml.load_soon(StreamModel, name="sample")
    loaded_model = model_future.result()

    assert loaded_model == original_model
    assert [v async for v in loaded_model.stream] == SAMPLE_DATA
