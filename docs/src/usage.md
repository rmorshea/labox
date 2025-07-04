# Usage

Lakery provides high-level and low-level APIs for saving and loading data. The
high-level API (`save_one`, `load_one`) is convenient for single operations, while the
low-level API (`data_saver`, `data_loader`) is more efficient for batch operations.

## Setup

Before you can save and load data with Lakery, you need to set up a database engine,
create the necessary database tables, and configure a registry with your storables,
serializers, and storages.

### Database Engine and Session

First, initialize an async SQLAlchemy engine. Here's an example using
[`aiosqlite`](https://pypi.org/project/aiosqlite/) for SQLite:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from lakery.core import BaseRecord

# Create the async engine
engine = create_async_engine("sqlite+aiosqlite:///lakery_data.db")

# Create a session maker
new_async_session = async_sessionmaker(engine, expire_on_commit=False)

# Create Lakery's database tables
await BaseRecord.create_all(engine)
```

For PostgreSQL, you might use:

```python
engine = create_async_engine("postgresql+asyncpg://user:password@localhost/lakery_db")
```

### Registry Configuration

Next, establish a [registry](./concepts/registry.md) with the
[storables](./concepts/storables.md), [serializers](./concepts/serializers.md), and
[storages](./concepts/storages.md) you plan to use:

```python
from lakery.core import Registry
from lakery.builtin.storages import FileStorage

# Basic registry with built-in components
registry = Registry(
    modules=["lakery.builtin"],
    default_storage=FileStorage("./lakery_storage", mkdir=True),
)
```

For more advanced setups, you can include additional modules and custom storages:

```python
from lakery.extra.os import FileStorage
from lakery.extra.aws import S3Storage

registry = Registry(
    modules=[
        "lakery.builtin",           # Core storables and serializers
        "lakery.extra.pydantic",    # Pydantic model support
        "lakery.extra.msgpack",     # MessagePack serialization
    ],
    storages=[
        FileStorage("./local_storage", mkdir=True),
        S3Storage("my-bucket", region="us-east-1"),
    ],
    default_storage=FileStorage("./default_storage", mkdir=True),
)
```

### Complete Setup Example

Here's a complete setup example you can use as a starting point:

```python
import asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from lakery.core import BaseRecord, Registry
from lakery.builtin.storages import FileStorage

async def setup_lakery():
    # Database setup
    engine = create_async_engine("sqlite+aiosqlite:///lakery_data.db")
    new_async_session = async_sessionmaker(engine, expire_on_commit=False)
    await BaseRecord.create_all(engine)

    # Registry setup
    registry = Registry(
        modules=["lakery.builtin"],
        default_storage=FileStorage("./lakery_storage", mkdir=True),
    )

    return new_async_session, registry

# Run setup
new_async_session, registry = asyncio.run(setup_lakery())
```

Now you're ready to start saving and loading data!

## Saving Storables

### Saving One

The simplest way to save a single object is with
[`save_one`][lakery.core.api.saver.save_one]:

```python
from lakery.core import save_one
from lakery.builtin import StorableValue

# Create some data to save
data = {"message": "Hello, world!", "value": 42}
obj = StorableValue(data)

# Save it
async with new_async_session() as session:
    record = await save_one(
        obj,
        registry=registry,
        session=session
    )
```

The function returns a [`ManifestRecord`][lakery.core.database.ManifestRecord] that
contains metadata about the saved object and can be used to load it later.

### Saving Many

For better performance when saving multiple objects, use the
[`data_saver`][lakery.core.api.saver.data_saver] context manager:

```python
from lakery.core import data_saver
from lakery.builtin import StorableValue

objects = [
    StorableValue({"id": 1, "name": "Alice"}),
    StorableValue({"id": 2, "name": "Bob"}),
    StorableValue({"id": 3, "name": "Charlie"}),
]

async with new_async_session() as session:
    async with data_saver(registry, session) as saver:
        futures = []
        for obj in objects:
            future = saver.save_soon(obj)
            futures.append(future)

    # All objects are saved when the context manager exits
    records = [future.result() for future in futures]
```

The `data_saver` schedules saves to run concurrently and commits them all to the
database when the context manager exits.

### Adding Tags

Tags are key-value pairs that provide metadata about saved objects. You can add tags at
two levels:

**Manifest-level tags** apply to the entire storable object:

```python
await save_one(
    obj,
    tags={"environment": "production", "version": "1.0"},
    registry=registry,
    session=session
)
```

**Content-level tags** apply to individual fields within a storable object. These are
specified in the storable's unpacker or through annotations:

```python
from lakery.extra.pydantic import StorageModel, StorageSpec
from typing import Annotated

class MyModel(StorageModel, class_id="my-model"):
    regular_field: Any
    tagged_field: Annotated[Any, StorageSpec(tags={"sensitive": "true"})]
```

Tags are merged when saving, with manifest-level tags taking priority over content-level
tags. Storage backends receive these merged tags and can use them for organization,
access control, or billing.

### Saving Streams

For large datasets that don't fit in memory, use
[`StorableStream`][lakery.builtin.storables.StorableStream]:

```python
from lakery.builtin import StorableStream

async def generate_data():
    for i in range(1000000):
        yield {"id": i, "value": f"item_{i}"}

stream_obj = StorableStream(generate_data())

async with new_async_session() as session:
    record = await save_one(stream_obj, registry=registry, session=session)
```

## Loading Storables

### Loading One

To load a single object, use [`load_one`][lakery.core.api.loader.load_one] with the
manifest record:

```python
from lakery.core import load_one

async with new_async_session() as session:
    loaded_obj = await load_one(
        record,
        StorableValue,  # Expected type (optional but recommended)
        registry=registry,
        session=session
    )

assert loaded_obj.value == data
```

If you don't specify the expected type, Lakery will infer it from the manifest record's
class ID.

### Loading Many

For efficient batch loading, use the [`data_loader`][lakery.core.api.loader.data_loader]
context manager:

```python
from lakery.core import data_loader

async with new_async_session() as session:
    async with data_loader(registry, session) as loader:
        futures = []
        for record in records:
            future = loader.load_soon(record, StorableValue)
            futures.append(future)

    # All objects are loaded when the context manager exits
    loaded_objects = [future.result() for future in futures]
```

### Loading Streams

When loading streams, the data is loaded lazily as you iterate:

```python
# Load the stream object
loaded_stream = await load_one(record, StorableStream, registry=registry, session=session)

# Iterate through the stream
async for item in loaded_stream.value_stream:
    print(item)
```

### Getting Manifests

You can query manifest records directly from the database to find objects to load:

```python
from sqlalchemy import select
from lakery.core.database import ManifestRecord

async with new_async_session() as session:
    # Find all manifests with specific tags
    stmt = select(ManifestRecord).where(
        ManifestRecord.tags["environment"].astext == "production"
    )

    results = await session.execute(stmt)
    manifests = results.scalars().all()

    # Load the objects
    async with data_loader(registry, session) as loader:
        futures = [loader.load_soon(manifest) for manifest in manifests]

    objects = [future.result() for future in futures]
```
