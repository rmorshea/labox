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

There's a [complete list of extras](./integrations/index.md) in the Integrations
section.

## Basic Setup

Initialize an async SQLAlchemy engine (in this case using
[`aiosqlite`](https://pypi.org/project/aiosqlite/)):

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///temp.db")
new_async_session = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()
```

Then use the engine to create Lakery's tables:

```python
from lakery.core import BaseRecord

BaseRecord.create_all(engine).run()
```

Establish a [registry](./concepts/registries.md) with the
[storables](./concepts/storables.md), [serializers](./concepts/serializers.md) and
[storages](./concepts/storages.md) you plan to use.

```python
from lakery.core import Registry
from lakery.extra.json import JsonSerializer
from lakery.extra.os import FileStorage

registry = Registry(
    "lakery.builtin",
    storages=[FileStorage("temp", mkdir=True)],
    use_default_storage=True,
)
```

## Basic Usage

With setup completed, find some data you want to save:

```python
data = {"hello": "world"}
```

Pick a [storable](./concepts/storables.md) to save it with:

```python
from lakery.builtin import StorableValue

obj = StorableValue(data)
```

Save the data and return a record of it:

```python
import asyncio

from lakery.core import data_saver


async def save():
    async with (
        new_async_session() as session,
        data_saver(session=session, registry=registry) as saver,
    ):
        future_record = saver.save_soon(obj)
    return future_record.result()


record = asyncio.run(main())
```

Load the data later using the record:

```python
import asyncio

from lakery.core import data_loader


async def load():
    async with (
        new_async_session() as session,
        data_loader(session=session, registry=registry) as loader,
    ):
        future_data = loader.load_soon(record)
    return future_data.result()


loaded_obj = asyncio.run(load())

assert loaded_obj == obj
```

Behind the scenes Lakery has automatically inferred an appropriate serializer and used
the default storage to save the data.
