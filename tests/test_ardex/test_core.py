from ardex.core.api.loader import data_loader
from ardex.core.api.saver import data_saver
from ardex.core.schema import DataRelation


async def test_simple_value_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async with data_saver() as save:
        rel_fut = save.value(DataRelation, input_data)
    rel = rel_fut.result()

    async with data_loader() as load:
        val_fut = load.value(rel)
    assert val_fut.result() == input_data

    async with data_loader() as load:
        stream_fut = load.stream(rel)
    assert [item async for item in stream_fut.result()] == input_data


async def test_simple_stream_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async def stream():
        for item in input_data:
            yield item

    async with data_saver() as save:
        rel_fut = save.stream(DataRelation, (dict, stream()))
    rel = rel_fut.result()

    async with data_loader() as load:
        value_fut = load.value(rel)
    assert value_fut.result() == input_data

    async with data_loader() as load:
        stream_fut = load.stream(rel)
    assert [item async for item in stream_fut.result()] == input_data
