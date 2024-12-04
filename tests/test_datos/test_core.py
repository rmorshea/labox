from ardex.core.loader import data_loader
from ardex.core.saver import data_saver
from ardex.core.schema import DataRelation


async def test_simple_scalar_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async with data_saver() as save:
        rel_fut = save.scalar(DataRelation, input_data)
    rel = rel_fut.result()

    async with data_loader() as load:
        val_fut = load.scalar(rel)
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
        scalar_fut = load.scalar(rel)
    assert scalar_fut.result() == input_data

    async with data_loader() as load:
        stream_fut = load.stream(rel)
    assert [item async for item in stream_fut.result()] == input_data
