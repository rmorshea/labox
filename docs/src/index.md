# <img src="./logo.svg" alt="Labox Logo" style="height:2em;position:relative;top:0.4em"> Labox

!!! warning

    Long-term support is not guaranteed.

A storage framework for heterogeneous data in Python.

## Installation

To install the core package:

```bash
pip install labox
```

To include the core package and extra integrations:

```bash
# all extras
pip install labox[all]

# or specific extras
pip install labox[pydantic,pandas,aws]
```

Be sure to checkout how Labox works with [Pydantic](./integrations/pydantic.md) as well
as all the other [integrations](./integrations/index.md) Labox offers.

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

Then use the engine to create Labox's tables:

```python
from labox.core import BaseRecord

BaseRecord.create_all(engine).run()
```

Establish a [registry](./concepts/registry.md) with the
[storables](./concepts/storables.md), [serializers](./concepts/serializers.md) and
[storages](./concepts/storages.md) you plan to use.

```python
from labox.core import Registry
from labox.extra.os import FileStorage

registry = Registry(
    modules=["labox.builtin", "labox.extra.pandas", "labox.extra.pydantic"],
    default_storage=FileStorage("temp", mkdir=True),
)
```

## Basic Usage

With setup completed, find some data you want to save:

```python
import pandas as pd

experiment_data = {
    "name": "Test Experiment",
    "results": pd.DataFrame(
        {"measurement_num": [1, 2, 3], "measurement_value": [1.23, 4.56, 7.89]}
    ),
}
```

Put that data in a [storable](./concepts/storables.md). In this case, a
[Pydantic](./integrations/pydantic.md) model:

```python
import pandas as pd

from labox.extra.pydantic import StorableModel


class ExperimentData(StorableModel, class_id="..."):  # (1)!
    name: str
    results: pd.DataFrame


experiment = ExperimentData(**experiment_data)
```

1. A `class_id` is a string that identifies a storable class when saving and loading
    data. You can use a placeholder like `"..."` until you
    [generate a unique ID](./concepts/storables.md#generating-ids). Once you've saved
    data with a specific `class_id`, it should never be changed or reused.

Save the data and return a [record](./concepts/database.md#manifest-records):

```python
from labox.core import save_one

async with new_async_session() as session:
    record = save_one(experiment, session=session, registry=registry)
```

Now, you can load the data back from the record:

```python
from labox.core import load_one

async with new_async_session() as session:
    loaded_experiment = load_one(record, session=session, registry=registry)

assert loaded_experiment == experiment
```

## Next Steps

Check out more [usage examples](./usage/index.md), or dive into the
[concepts](./concepts/index.md) and [integrations](./integrations/index.md) to learn
more about how Labox works.
