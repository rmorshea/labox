from datos.core.loader import data_loader
from datos.core.saver import data_saver
from datos.core.schema import DataRelation


async def test_simple_scalar_data_saver_and_loader_usage():
    async with data_saver() as save:
        rel_fut = save.scalar(DataRelation, "Hello, Bob!")
    rel = rel_fut.result()

    async with data_loader() as load:
        val_fut = load.scalar(rel)
    assert val_fut.result() == "Hello, Bob!"


async def test_simple_stream_data_saver_and_loader_usage():
    input_data = [{"message": "Hello, Alice!"}, {"message": "Goodbye, Alice!"}]

    async def stream():
        for item in input_data:
            yield item

    async with data_saver() as save:
        rel_fut = save.stream(DataRelation, (dict, stream()))
    rel = rel_fut.result()

    async with data_loader() as load:
        val_fut = load.scalar(rel)
    assert val_fut.result() == input_data
