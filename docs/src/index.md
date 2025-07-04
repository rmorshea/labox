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
section, but be sure to checkout how Lakery works with
[Pydantic](./integrations/3rd-party/pydantic.md).

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

Establish a [registry](./concepts/registry.md) with the
[storables](./concepts/storables.md), [serializers](./concepts/serializers.md) and
[storages](./concepts/storages.md) you plan to use.

```python
from lakery.core import Registry
from lakery.extra.os import FileStorage

registry = Registry(
    modules=["lakery.builtin"],
    default_storage=FileStorage("temp", mkdir=True),
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


async def save(obj):
    async with new_async_session() as session:
        return save_one(obj, session=session, registry=registry)


record = asyncio.run(save(obj))
```

Behind the scenes Lakery inferred an appropriate serializer (in this case
[JSON](./integrations/built-ins/serializers.md#json)) and default storage from the
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

## With Pydantic

Lakery works well with [Pydantic](https://pydantic.dev/), allowing you to save and load
[Pydantic models](https://docs.pydantic.dev/latest/) as `Storable` objects. First be
sure a compatible version of Pydantic is installed:

```bash
pip install lakery[pydantic]
```

Then add Pydantic to your registry:

```python
registry = Registry(
    modules=["lakery.builtin", "lakery.extra.pydantic"],
    default_storage=FileStorage("temp", mkdir=True),
)
```

Now define a model that inherits from `StorableModel`:

```python
from lakery.extra.pydantic import StorableModel


class MyModel(StorableModel, class_id="abc123"):
    name: str
    value: int
```

You can then save and load instances of this model just like any other `Storable`:

```python
from lakery.core import load_one
from lakery.core import save_one


async def save_model(model):
    async with new_async_session() as session:
        return await save_one(model, session=session, registry=registry)


async def load_model(record):
    async with new_async_session() as session:
        return await load_one(record, session=session, registry=registry)


# Example usage
model = MyModel(name="Test", value=42)
record = asyncio.run(save_model(model))
loaded_model = asyncio.run(load_model(record))
assert loaded_model == model
```

Lakery also works with Pydantic's powerful type annotation features. For example, you
can annotate that certain fields should use a specific serializer or storage with the
`StorableSpec` annotation. Here is an example using a Pandas DataFrame with a Parquet
serializer:

```python
from typing import Annotated

import pandas as pd

from lakery.extra.pandas import ParquetDataFrameSerializer
from lakery.extra.pydantic import StorableSpec

DataFrame = Annotated[pd.DataFrame, StorableSpec(serializer=ParquetDataFrameSerializer)]


class MyModel(StorableModel, class_id="abc123"):
    name: str
    data: DataFrame
```

!!! note

    Be sure to add the `ParquetDataFrameSerializer` to your registry.
