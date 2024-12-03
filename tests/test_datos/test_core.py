from datos.core.loader import data_loader
from datos.core.saver import data_saver
from datos.core.schema import DataRelation


async def test_simple_scalar_data_saver_and_loader_usage():
    async with data_saver() as save:
        rel_fut = save.scalar(DataRelation, "Hello, World!")
    rel = rel_fut.result()

    async with data_loader() as load:
        val_fut = load.scalar(rel)
    assert val_fut.result() == "Hello, World!"
