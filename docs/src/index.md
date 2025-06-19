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

from lakery.core import save_one


async def save():
    async with new_async_session() as session
        return save_one(obj, session=session, registry=registry)


record = asyncio.run(main())
```

Behind the scenes Lakery inferred an appropriate serializer and default storage from the
registry you created in the [setup](#basic-setup) section. Where and how the data was
stored is recorded in the the [database](./concepts/database.md) so you can retrieve it
later:

```python
import asyncio

from lakery.core import load_one


async def load(record):
    async with new_async_session() as session:
        return load_one(record, session=session, registry=registry)


loaded_obj = asyncio.run(load(record))
assert loaded_obj == obj
```
