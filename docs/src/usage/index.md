# Overview

## Database Setup

To use Lakery, you'll need to have set up the Lakery [database](./concepts/database.md)
scheme as well as establish a
[SQLAlchemy connection](https://docs.sqlalchemy.org/en/20/orm/session_basics.html) to
it. The latter is typically done using an async SQLAlchemy engine and session maker.To
setup the schema SQLAlchemy recommends managing migrations with a tool like
[alembic](https://alembic.sqlalchemy.org/en/latest/). But for the sake of simplicity,
you can create the tables directly using the `create_all` method of Lakery's
`BaseRecord` class. This will create the necessary tables in the database.

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///temp.db")
new_async_session = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()
```

## Registry Setup

When saving and loading data, Lakery makes use of [registry](./concepts/registry.md) to
know what [storables](./concepts/storables.md), [unpackers](./concepts/unpackers.md),
and [serializers](./concepts/serializers.md) and [storages](./concepts/storages.md) are
available. A quick way to set up a registry is to
[construct it from the modules](../concepts/registry.md#constructing-from-modules) where
these components are defined.

```python
from lakery import Registry

registry = Registry(
    modules=[
        "lakery.builtin",
        "lakery.extra.pydantic",
    ]
)
```

## Storable Setup

There's two main ways to create a [storable](./concepts/storables.md).

With [Pydantic models](../integrations/3rd-party/pydantic.md).

```python
from lakery.extra.pydantic import StorableModel


class ExperimentData(StorableModel):
    description: str
    parameters: dict[str, float]
    results: dict[str, list[float]]
```

Or with [dataclasses](../integrations/built-ins/storables.md#dataclasses).

```python
from dataclasses import dataclass
from lakery.builtin import StorableDataclass


@dataclass
class ExperimentData(StorableDataclass):
    description: str
    parameters: dict[str, float]
    results: dict[str, list[float]]
```

Both look similar, but Pydantic models allow for more complex serialization and storage
options. For example, if you cannot nest non-JSON compatible types inside the value of a
field. In this case, if you swapped out `dict[str, list[float]]` for
`dict[str, np.ndarray]` the dataclass only knows how to serialize the outer-most
structure (the `dict`), but not the inner structure (the `np.ndarray`). The underlying
schema of Pydantic models does not have this limitation, so you can use
`dict[str, np.ndarray]` directly without any issues:

```python
import numpy as np
from lakery.extra.pydantic import StorableModel


class ExperimentData(StorableModel):
    description: str
    parameters: dict[str, float]
    results: dict[str, np.ndarray]
```

## Saving Storables

### Saving One

If you have a single storable to save you can use the [`save_one`][lakery.core.save_one]
function. To call it you'll need a [SQLAlchemy session](#database-setup) and
[Lakery registry](#registry-setup). Once the object has been saved it will return a
[record](./concepts/database.md#manifest-records) that can be used to
[load](#loading-one) the storable later.

```python
from lakery.core import Storable
from lakery.core import save_one

obj = ExperimentData(
    description="Experiment 1",
    parameters={"learning_rate": 0.01, "batch_size": 32},
    results={"accuracy": [0.8, 0.85, 0.9], "loss": [0.5, 0.4, 0.3]},
)
async with new_async_session() as session:
    record = save_one(obj, session=session, registry=registry)
```

### Saving in Bulk

To save many storables at once you can use the [`new_saver`][lakery.core.new_saver]
context manager. This will create a saver object that's able to save multiple storables
concurrently. As above, you'll need a [SQLAlchemy session](#database-setup) and
[Lakery registry](#registry-setup). The saver's `save_soon` method accepts a single
storable and returns a future that will, once the context exits, resolve to a
[record](./concepts/database.md#manifest-records) for that storable. The records can
then be used to [load](#loading-in-bulk) the storables later.

```python
from lakery.core import Storable
from lakery.core import new_saver

objs = [
    ExperimentData(
        description="Experiment 1",
        parameters={"learning_rate": 0.01, "batch_size": 32},
        results={"accuracy": [0.8, 0.85, 0.9], "loss": [0.5, 0.4, 0.3]},
    ),
    ExperimentData(
        description="Experiment 2",
        parameters={"learning_rate": 0.001, "batch_size": 64},
        results={"accuracy": [0.75, 0.8, 0.85], "loss": [0.6, 0.5, 0.4]},
    ),
]
async with new_async_session() as session:
    async with new_saver(session=session, registry=registry) as ms:
        future_records = [ms.save_soon(s) for s in objs]
    record = [r.get() for r in future_records]
```

## Loading Storables

### Loading One

If you have a single record to load you can use the [`load_one`][lakery.core.load_one]
function. You'll need the [record](./concepts/database.md#manifest-records) returned
from saving, a [SQLAlchemy session](#database-setup) and
[Lakery registry](#registry-setup). The function will return the original storable
object.

```python
from lakery.core import load_one

# Using the record from saving above
async with new_async_session() as session:
    loaded_obj = await load_one(record, ExperimentData, session=session, registry=registry)
```

### Loading in Bulk

To load many storables at once you can use the [`new_loader`][lakery.core.new_loader]
context manager. This will create a loader object that's able to load multiple storables
concurrently. As above, you'll need the
[records](./concepts/database.md#manifest-records) from saving, a
[SQLAlchemy session](#database-setup) and [Lakery registry](#registry-setup). The
loader's `load_soon` method accepts a record and storable type, returning a future that
will, once the context exits, resolve to the original storable object.

```python
from lakery.core import new_loader

# Using the records from saving above
async with new_async_session() as session:
    async with new_loader(session=session, registry=registry) as ml:
        future_objs = [ml.load_soon(r, ExperimentData) for r in records]
    loaded_objs = [future.get() for future in future_objs]
```

## Adding Tags

### Manifest Tags

You can add tags to manifest records when saving storables. These tags are associated
with the entire storable and can be used for filtering and organization. Tags are
provided as a dictionary of string key-value pairs.

```python
from lakery.core import save_one

obj = ExperimentData(
    description="Experiment 1",
    parameters={"learning_rate": 0.01, "batch_size": 32},
    results={"accuracy": [0.8, 0.85, 0.9], "loss": [0.5, 0.4, 0.3]},
)
async with new_async_session() as session:
    record = save_one(
        obj,
        session=session,
        registry=registry,
        tags={"experiment_type": "baseline", "model": "cnn"}
    )
```

This is similarly possible when saving in bulk with the `new_saver` context manager's
`saver.save_soon` method.

### Content Tags

Content tags can be applied to values within a storable when using if a separate storage
location is desired for that piece of content. This is done using the `ContentSpec`
class from `lakery.extra.pydantic`. The `ContentSpec` is meant to be used as
[`Annotated`][typing.Annotated] metadata for the field.

```python
from typing import Annotated
from lakery.extra.pydantic import StorableModel, ContentSpec
from lakery.extra.aws import S3Storage

class ExperimentData(StorableModel):
    description: str
    parameters: dict[str, float]
    results: Annotated[dict[str, np.ndarray], ContentSpec(storage=S3Storage, tags={"type": "results"})]
```
