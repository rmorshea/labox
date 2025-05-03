# Lakery

A data storage framework for Python.

## Installation

```bash
pip install lakery  # core package
```

```bash
pip install lakery[all]  # all built-in extras
```

See the "Integrations" for the full list of extras.

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
from lakery.core import ModelRegistry
from lakery.core import Registries
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.os import FileStorage

serializers = SerializerRegistry([JsonSerializer()])
storages = StorageRegistry(default=FileStorage("temp", mkdir=True))
models = ModelRegistry.from_modules("lakery.common.models")  # include built-in models
registries = Registries(serializers=serializers, storages=storages)
```

Save and load some data:

```python
import asyncio

from lakery.core import data_loader
from lakery.core import data_saver


async def main():
    data = Singular({"hello": "world"})

    async with AsyncSession() as session:
        async with data_saver(registries=registries, session=session) as saver:
            future_record = saver.save_soon(data)
        record = future_record.result()

        with data_loader(registries=registries, session=session) as loader:
            future_data = loader.load_soon(record)
        loaded_data = future_data.result()

    assert loaded_data == data


asyncio.run(main())
```
