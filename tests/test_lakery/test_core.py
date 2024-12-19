from lakery.core.api.loader import data_loader
from lakery.core.api.saver import data_saver
from lakery.core.schema import DataRelation


async def test_simple_value_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async with data_saver() as save:
        rel_fut = save.value(DataRelation, input_data)
    rel = rel_fut.result()

    load = data_loader()
    assert (await load.value(rel)) == input_data
    assert [item async for item in load.stream(rel)] == input_data


async def test_simple_stream_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async def stream():
        for item in input_data:
            yield item

    async with data_saver() as save:
        rel_fut = save.stream(DataRelation, (dict, stream()))
    rel = rel_fut.result()

    load = data_loader()
    assert (await load.value(rel)) == input_data
    assert [item async for item in load.stream(rel)] == input_data
