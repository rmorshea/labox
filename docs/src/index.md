# Lakery

A data storage framework for Python.

## Installation

```bash
pip install lakery  # core package
```

```bash
pip install lakery[all]  # all built-in extras
```

See [integrations](integrations.md) for a list of available extras.

## Basic Usage

Set up your async SQLAlchemy engine and create Lakery's tables:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from lakery.core.schema import BaseRecord

engine = create_async_engine("sqlite+aiosqlite:///temp.db")
AsyncSession = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()
```

Pick your serializers and storages:

```python
from lakery.core import Registries
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry

serializers = SerializerRegistry([JsonSerializer()])
storages = StorageRegistry([FileStorage("temp", mkdir=True)])
registries = Registries(serializers=serializers, storages=storages)
```

Save and load some data:

```python
import asyncio

from lakery.core import data_loader
from lakery.core import data_saver


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
```
