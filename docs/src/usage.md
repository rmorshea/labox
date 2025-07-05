# Usage

## Setup

### SQLAlchemy Engine

### Component Registry

### Complete Example

## Saving Storables

### Saving One

### Saving Many

### With Pydantic

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

### Adding Tags

#### Manifest Tags

#### Content Tags

## Loading Storables

### Loading One

### Loading Many

### Loading Streams

### Getting Manifests
