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

There's a [complete list of extras](./integrations) in the Integrations section, but be
sure to checkout how Lakery works with [Pydantic](./integrations/3rd-party/pydantic.md).

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
    modules=["lakery.builtin", "lakery.extra.pandas", "lakery.extra.pydantic"],
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

Put that data in a [storable](./concepts/storables.md), in this case a
[Pydantic](./integrations/3rd-party/pydantic.md) model:

```python
import pandas as pd

from lakery.extra.pydantic import StorableModel


class ExperimentData(StorableModel, class_id="abc123"):
    name: str
    results: pd.DataFrame


experiment = ExperimentData(**experiment_data)
```

Save the data and return a [record](./concepts/database.md#manifest-records):

```python
from lakery.core import save_one

async with new_async_session() as session:
    record = save_one(experiment, session=session, registry=registry)
```

Now, you can load the data back from the record:

```python
from lakery.core import load_one

async with new_async_session() as session:
    loaded_experiment = load_one(record, session=session, registry=registry)

assert loaded_experiment == experiment
```

## Next Steps

Check out more [usage examples](./usage), or dive into the [concepts](./concepts) and
[integrations](./integrations) to learn more about how Lakery works.
