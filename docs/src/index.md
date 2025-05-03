# Lakery

A data storage framework for Python.

## Installation

To install the core package:

```bash
pip install lakery
```

To include the core package and extra integrations:

```bash
# all extras
pip install lakery[all]

# or specific extras
pip install lakery[pydantic,pandas,aws]
```

There's a [complete list of extras](./integrations) in the Integrations section.

## Basic Setup

Create an async SQLAlchemy engine (in this case using
[`aiosqlite`](https://pypi.org/project/aiosqlite/)) and Lakery's tables:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from lakery.core.schema import BaseRecord

engine = create_async_engine("sqlite+aiosqlite:///temp.db")
new_async_session = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()
```

Create a registry with the [serializers](./concepts/serializers.md),
[storages](.concepts/storages.md), and [models](./concepts/models.md) you need.

```python
from lakery.core import ModelRegistry
from lakery.core import Registries
from lakery.core import SerializerRegistry
from lakery.core import StorageRegistry
from lakery.extra.json import JsonSerializer
from lakery.extra.os import FileStorage

serializers = SerializerRegistry([JsonSerializer()])
storages = StorageRegistry(default=FileStorage("temp", mkdir=True))
models = ModelRegistry.from_modules("lakery.common.models")

registries = Registries(serializers=serializers, storages=storages, models=models)
```

## Basic Usage

Find some data you want to save:

```python
data = {"hello": "world"}
```

Pick a [model](./concepts/models.md) to save it with:

```python
from lakery.common.models import Singular

model = Singular(data)
```

Save the data to a storage (by default the `FileStorage` in the storage registry) and
return a record of it:

```python
import asyncio

from lakery.core import data_saver


async def save():
    async with new_async_session() as session:
        async with data_saver(session=session, registries=registries) as saver:
            future_record = saver.save_soon(model)
    return future_record.result()


record = asyncio.run(main())
```

Look and load the data using the record:

```python
import asyncio

from lakery.core import data_loader


async def load():
    async with new_async_session() as session:
        async with data_loader(session=session, registries=registries) as loader:
            future_data = loader.load_soon(record)
    return future_data.result()


loaded_model = asyncio.run(load())

assert loaded_model == model
```
