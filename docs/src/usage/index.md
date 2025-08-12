# Overview

## Database Setup

To use Labox, you'll need to have set up the Labox [database](./concepts/database.md)
scheme as well as establish a
[SQLAlchemy connection](https://docs.sqlalchemy.org/en/20/orm/session_basics.html) to
it. The latter is typically done using an async SQLAlchemy engine and session maker.To
setup the schema SQLAlchemy recommends managing migrations with a tool like
[alembic](https://alembic.sqlalchemy.org/en/latest/). But for the sake of simplicity,
you can create the tables directly using the `create_all` method of Labox's `BaseRecord`
class. This will create the necessary tables in the database.

```python
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

engine = create_async_engine("sqlite+aiosqlite:///temp.db")
new_async_session = async_sessionmaker(engine, expire_on_commit=True)
BaseRecord.create_all(engine).run()
```

## Registry Setup

When saving and loading data, Labox makes use of [registry](./concepts/registry.md) to
know what [storables](./concepts/storables.md), [unpackers](./concepts/unpackers.md),
and [serializers](./concepts/serializers.md) and [storages](./concepts/storages.md) are
available. A quick way to set up a registry is to
[construct it from the modules](../concepts/registry.md#constructing-from-modules) where
these components are defined.

```python
from labox import Registry

registry = Registry(
    modules=[
        "labox.builtin",
        "labox.extra.pydantic",
        "labox.extra.pandas",
    ]
)
```

## Storable Setup

There's two main ways to create a [storable](./concepts/storables.md).

With [Pydantic models](../integrations/3rd-party/pydantic.md).

```python
from labox.extra.pydantic import StorableModel


class ExperimentData(StorableModel):
    description: str
    parameters: dict[str, float]
    results: dict[str, list[float]]
```

Or with [dataclasses](../integrations/built-ins/storables.md#dataclasses).

```python
from dataclasses import dataclass

from labox.builtin import StorableDataclass


@dataclass
class ExperimentData(StorableDataclass):
    description: str
    parameters: dict[str, float]
    results: dict[str, list[float]]
```

Both look similar, but Pydantic models allow for more complex serialization and storage
options. For example, you cannot nest non-JSON compatible types inside the value of a
field. In this case, if you swapped out `dict[str, list[float]]` for
`dict[str, np.ndarray]` the dataclass only knows how to serialize the outer-most
structure (the `dict`), but not the inner structure (the `np.ndarray`). Pydantic models
do not have this limitation, so you can use `dict[str, np.ndarray]` directly without any
issues:

```python
import numpy as np

from labox.extra.pydantic import StorableModel


class ExperimentData(StorableModel):
    description: str
    parameters: dict[str, float]
    results: dict[str, np.ndarray]
```

## Saving Storables

### Saving One

If you have a single storable to save you can use the [`save_one`][labox.core.save_one]
function. To call it you'll need a [SQLAlchemy session](#database-setup) and
[Labox registry](#registry-setup). Once the object has been saved it will return a
[record](./concepts/database.md#manifest-records) that can be used to
[load](#loading-one) the storable later.

```python
from labox.core import save_one

obj = ExperimentData(
    experiment_name="protein_folding_analysis_trial_1",
    parameters={"temperature": 298.15, "ph": 7.4, "concentration": 0.1},
    results={"binding_affinity": [12.3, 15.7, 9.8], "stability": [85.2, 78.9, 92.1]},
)
async with new_async_session() as session:
    record = save_one(obj, session=session, registry=registry)
```

### Saving in Bulk

To save many storables at once you can use the [`new_saver`][labox.core.new_saver]
context manager. This will create a saver object that's able to save multiple storables
concurrently. As above, you'll need a [SQLAlchemy session](#database-setup) and
[Labox registry](#registry-setup). The saver's `save_soon` method accepts a single
storable and returns a future that will, once the context exits, resolve to a
[record](./concepts/database.md#manifest-records) for that storable. The records can
then be used to [load](#loading-in-bulk) the storables later.

```python
from labox.core import new_saver

objs = [
    ExperimentData(
        experiment_name="protein_folding_analysis_trial_1",
        parameters={"temperature": 298.15, "ph": 7.4, "concentration": 0.1},
        results={"binding_affinity": [12.3, 15.7, 9.8], "stability": [85.2, 78.9, 92.1]},
    ),
    ExperimentData(
        experiment_name="protein_folding_analysis_trial_2",
        parameters={"temperature": 310.15, "ph": 7.2, "concentration": 0.2},
        results={"binding_affinity": [14.1, 9.3, 11.2], "stability": [79.8, 83.4, 88.7]},
    ),
]
async with new_async_session() as session:
    async with new_saver(session=session, registry=registry) as ms:
        futures = [ms.save_soon(s) for s in objs]
    records = [f.value for f in futures]
```

### Saving with Streams

Storables may contain async streams of data. For example the storable below contains a
stream of data that is generated on the fly:

## Loading Storables

### Loading One

If you have a single record to load you can use the [`load_one`][labox.core.load_one]
function. You'll need the [record](./concepts/database.md#manifest-records) returned
from saving, a [SQLAlchemy session](#database-setup) and
[Labox registry](#registry-setup). The function will return the original storable
object.

```python
from labox.core import load_one

# Using the record from saving above
async with new_async_session() as session:
    loaded_obj = await load_one(record, ExperimentData, session=session, registry=registry)
```

### Loading in Bulk

To load many storables at once you can use the [`new_loader`][labox.core.new_loader]
context manager. This will create a loader object that's able to load multiple storables
concurrently. As above, you'll need the
[records](./concepts/database.md#manifest-records) from saving, a
[SQLAlchemy session](#database-setup) and [Labox registry](#registry-setup). The
loader's `load_soon` method accepts a record and storable type, returning a future that
will, once the context exits, resolve to the original storable object.

```python
from labox.core import new_loader

# Using the records from saving above
async with new_async_session() as session:
    async with new_loader(session=session, registry=registry) as ml:
        futures = [ml.load_soon(r, ExperimentData) for r in records]
    loaded_objs = [f.value for f in futures]
```

### Loading with Streams

If the

## Adding Tags

You can add tags when saving storables. These tags are included in the
[manifest record](./concepts/database.md#manifest-records) and passed to the underlying
storage when saving each piece of content. Tags are provided as a dictionary of string
key-value pairs. This is useful for adding metadata to your records, such billing
information, project names, or any other relevant information that can help you identify
and manage your saved data.

```python
from labox.core import save_one

obj = ExperimentData(
    experiment_name="protein_folding_analysis_trial_1",
    parameters={"temperature": 298.15, "ph": 7.4, "concentration": 0.1},
    results={"binding_affinity": [12.3, 15.7, 9.8], "stability": [85.2, 78.9, 92.1]},
)
async with new_async_session() as session:
    record = save_one(
        obj,
        session=session,
        registry=registry,
        tags={
            "funding_source": "nsf_grant_12345",
            "project": "protein_dynamics_2024",
            "phase": "initial_screening",
        },
    )
```

This is similarly possible when saving in bulk with the `new_saver` context manager's
`saver.save_soon` method.
