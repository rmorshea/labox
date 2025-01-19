import asyncio

from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from lakery.core import Registries
from lakery.core import Scalar
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry
from lakery.core import data_loader
from lakery.core import data_saver
from lakery.core.schema import BaseRecord
from lakery.extra.json import JsonSerializer
from lakery.extra.os import FileStorage

# Setup your async SQLAlchemy engine and create Lakery's tables
engine = create_async_engine("sqlite+aiosqlite:///temp.db")
AsyncSession = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()

# Pick your serializers and storages
serializers = SerializerRegistry([JsonSerializer()])
storages = StorageRegistry([FileStorage("temp", mkdir=True)])
registries = Registries(serializers=serializers, storages=storages)


async def main():
    data = Scalar({"hello": "world"})

    async with AsyncSession() as session:
        async with data_saver(registries=registries, session=session) as saver:
            future_record = saver.save_soon(data)
        record = future_record.result()

        with data_loader() as loader:
            future_data = loader.load_soon(record)
        loaded_data = future_data.result()

    assert loaded_data == data


asyncio.run(main())
